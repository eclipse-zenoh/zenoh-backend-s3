//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

mod client;
pub mod config;

use async_std::sync::Arc;
use async_trait::async_trait;

use aws_sdk_s3::model::Object;
use aws_sdk_s3::output::GetObjectOutput;
use client::S3Client;
use futures::future::join_all;
use std::convert::TryFrom;

use crate::config::S3Config;
use futures::stream::FuturesUnordered;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::properties::Properties;
use zenoh::time::Timestamp;
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{StorageConfig, VolumeConfig};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_backend_traits::*;
use zenoh_core::zerror;

// Properties used by the Backend
pub const PROP_S3_ENDPOINT: &str = "url";
pub const PROP_S3_REGION: &str = "region";

// Amount of worker threads to be used by the tokio runtime of the [S3Storage] to handle incoming
// operations.
const STORAGE_WORKER_THREADS: usize = 2;

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static! {
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
}

#[allow(dead_code)]
const CREATE_BACKEND_TYPECHECK: CreateVolume = create_volume;

#[no_mangle]
pub fn create_volume(mut config: VolumeConfig) -> ZResult<Box<dyn Volume>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();
    log::debug!("S3 Backend {}", LONG_VERSION.as_str());

    config
        .rest
        .insert("version".into(), LONG_VERSION.clone().into());

    let endpoint = get_optional_string_property(PROP_S3_ENDPOINT, &config)?;
    let region = get_optional_string_property(PROP_S3_REGION, &config)?;

    let mut properties = Properties::default();
    properties.insert("version".into(), LONG_VERSION.clone());

    let admin_status = properties
        .0
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();

    Ok(Box::new(S3Backend {
        admin_status,
        endpoint,
        region,
    }))
}

fn get_optional_string_property(property: &str, config: &VolumeConfig) -> ZResult<Option<String>> {
    match config.rest.get(property) {
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        None => {
            log::debug!("Property '{property}' was not specified. ");
            Ok(None)
        }
        _ => Err(zerror!("Property '{property}' for S3 Backend must be a string.").into()),
    }
}

pub struct S3Backend {
    admin_status: serde_json::Value,
    endpoint: Option<String>,
    region: Option<String>,
}

#[async_trait]
impl Volume for S3Backend {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    async fn create_storage(&mut self, config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        log::debug!("Creating storage {:?}", config);
        let config: S3Config = S3Config::new(&config).await?;

        let client = S3Client::new(
            config.credentials.to_owned(),
            config.bucket.to_owned(),
            self.region.to_owned(),
            self.endpoint.to_owned(),
        )
        .await;

        client
            .create_bucket(config.reuse_bucket_is_enabled)
            .map_err(|e| zerror!("Couldn't create storage: {e}"))?
            .map_or_else(
                || log::debug!("Reusing existing bucket '{}'.", client),
                |_| log::debug!("Bucket '{}' successfully created.", client),
            );

        let storage_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(STORAGE_WORKER_THREADS)
            .enable_all()
            .build()
            .map_err(|e| zerror!(e))?;

        log::debug!("Tokio runtime created for storage operations.");

        Ok(Box::new(S3Storage {
            config,
            client: Arc::new(client),
            runtime: storage_runtime,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }
}

struct S3Storage {
    config: S3Config,
    client: Arc<S3Client>,
    runtime: tokio::runtime::Runtime,
}

#[async_trait]
impl Storage for S3Storage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.config.admin_status.to_owned()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<StorageInsertionResult> {
        log::debug!(
            "'{}' called on client {}. Key: '{}'",
            sample.kind,
            self.client,
            sample.key_expr
        );
        let key = sample
            .key_expr
            .as_str()
            .strip_prefix(&self.config.path_prefix)
            .ok_or_else(|| {
                zerror!(
                    "Received a Sample not starting with path_prefix '{}'",
                    self.config.path_prefix
                )
            })?;

        match sample.kind {
            SampleKind::Put => {
                if !self.config.is_read_only {
                    let client2 = self.client.clone();
                    let sample2 = sample.to_owned();
                    let key2 = key.to_owned();
                    self.runtime
                        .spawn(async move { client2.put_object(key2, sample2).await })
                        .await
                        .map_err(|e| zerror!("Put operation failed: {e}"))?
                        .map_err(|e| zerror!("Put operation failed: {e}"))?;
                    Ok(StorageInsertionResult::Inserted)
                } else {
                    log::warn!("Received PUT for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Delete => {
                if !self.config.is_read_only {
                    let client2 = self.client.clone();
                    let key2 = key.to_owned();

                    self.runtime
                        .spawn(async move { client2.delete_object(key2.to_string()).await })
                        .await
                        .map_err(|e| zerror!("Delete operation failed: {e}"))?
                        .map_err(|e| zerror!("Delete operation failed: {e}"))?;
                    Ok(StorageInsertionResult::Deleted)
                } else {
                    log::warn!("Received DELETE for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
        }
    }

    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        let key = query
            .key_expr()
            .as_str()
            .strip_prefix(&self.config.path_prefix)
            .ok_or_else(|| {
                zerror!(
                    "Received a Query not starting with path_prefix '{}'",
                    self.config.path_prefix
                )
            })?;

        log::debug!(
            "Query operation received for '{}' on bucket '{}'.",
            query.key_expr().as_str(),
            self.client
        );

        let key_expr = query.key_expr();
        if key_expr.is_wild() {
            let client = self.client.clone();
            let key_expr = key_expr.to_owned();
            let prefix = self.config.path_prefix.to_owned();

            let arc_query = Arc::new(query);
            let intersecting_objects = self
                .runtime
                .spawn(async move { get_intersecting_objects(&client, &key_expr, &prefix).await })
                .await
                .map_err(|e| zerror!("Get operation failed: {e}"))?
                .map_err(|e| zerror!("Get operation failed: {e}"))?;

            join_all(
                intersecting_objects
                    .into_iter()
                    .map(|object| {
                        let client = self.client.clone();
                        let query = arc_query.clone();
                        let prefix = self.config.path_prefix.to_owned();
                        self.runtime.spawn(async move {
                            let result = get_value_from_storage(client, object).await;
                            match result {
                                Ok(value) => reply_query(query, prefix, value.0, value.1).await,
                                Err(err) => {
                                    log::debug!("Unable to retrieve object from storage: {err:?}");
                                    Ok(())
                                }
                            }
                        })
                    })
                    .collect::<FuturesUnordered<_>>(),
            )
            .await;
        } else {
            let value = get_stored_value(self, key).await?;
            query
                .reply(Sample::new(query.key_expr().clone(), value))
                .res()
                .await
                .map_err(|e| zerror!("{e}"))?
        }
        Ok(())
    }

    // TODO(https://github.com/DariusIMP/zenoh-backend-s3/issues/1): create
    // mechanism to store the Timestamp id and time on the bucket files in
    // order to retrieve them here below.
    async fn get_all_entries(&self) -> ZResult<Vec<(OwnedKeyExpr, Timestamp)>> {
        log::debug!(
            "Issue 'https://github.com/DariusIMP/zenoh-backend-s3/issues/1' 
        needs to be solved first before being able to retrieve all the entries."
        );
        Err("Could not retrieve all entries from storage.".into())
    }
}

async fn get_stored_value(storage: &S3Storage, key: &str) -> ZResult<Value> {
    let client2 = storage.client.clone();
    let key2 = key.to_owned();

    let output_result = storage
        .runtime
        .spawn(async move { client2.get_object(key2.as_str()).await })
        .await
        .map_err(|e| zerror!("Get operation failed: {e}"))?
        .map_err(|e| zerror!("Get operation failed: {e}"))?;

    let encoding = output_result.content_encoding().map(|x| x.to_string());
    let bytes = output_result
        .body
        .collect()
        .await
        .map(|data| data.into_bytes())
        .map_err(|e| zerror!("Get operation failed. Couldn't process retrieved contents: {e}"))?;

    let value = match encoding {
        Some(encoding) => Encoding::try_from(encoding).map_or_else(
            |_| Value::from(Vec::from(bytes.to_owned())),
            |result| Value::from(Vec::from(bytes.to_owned())).encoding(result),
        ),
        None => Value::from(Vec::from(bytes)),
    };
    Ok(value)
}

impl Drop for S3Storage {
    fn drop(&mut self) {
        match self.config.on_closure {
            config::OnClosure::DestroyBucket => {
                let client2 = self.client.clone();
                async_std::task::spawn(async move {
                    client2.delete_bucket().await.map_or_else(
                        |e| {
                            log::debug!(
                                "Error while closing S3 storage '{}': {}",
                                client2,
                                e.to_string()
                            )
                        },
                        |_| log::debug!("Closing S3 storage '{}'", client2),
                    );
                });
            }
            config::OnClosure::DoNothing => {
                log::debug!(
                    "Close S3 storage, keeping bucket '{}' as it is.",
                    self.client
                );
            }
        }
    }
}

/// Utility function to retrieve the intersecting objects on the S3 storage with a wild key
/// expression.
///
/// # Arguments
///
/// * `client` - the [S3Client] allowing us to communicate with the S3 server
/// * `key_expr` - the wild key expression we want to intersect against the stored keys
/// * `prefix` - prefix from the configuration
///
/// # Returns
///
/// A vector of the intersecting objects contained in the storage or an error result. Note that the
/// objects do not contain their actual paylod, this must be retrieved afterwards doing a proper
/// request.
async fn get_intersecting_objects(
    client: &S3Client,
    key_expr: &KeyExpr<'_>,
    prefix: &String,
) -> ZResult<Vec<Object>> {
    let mut intersecting_objects_metadata = Vec::new();
    let objects_metadata = client.list_objects_in_bucket().await?;
    for metadata in objects_metadata {
        let key = KeyExpr::try_from(prefix.to_owned() + "/" + metadata.key().unwrap())?;
        if key_expr.intersects(key.as_ref()) {
            intersecting_objects_metadata.push(metadata);
        }
    }
    Ok(intersecting_objects_metadata)
}

/// Utility function to retrieve the value of an object to the S3 storage.
///
/// This function obtains the key from the object metadata and realizes a request upon the S3
/// server to obtain the actual payload and thus its value. It is intended to be used by multiple
/// tasks running in parallel.
///
/// # Arguments
///
/// * `client` - atomically reference counted of the [S3Client] with which we communicate with the
///             s3 server
/// * `object_metadata` - metadata of the object from which the value is to be retrieved
///
/// # Returns
///
/// * A (String, Value) tupple where the value is the object's value and the string parameter is
///     the key of the object.
async fn get_value_from_storage(
    client: Arc<S3Client>,
    object_metadata: Object,
) -> ZResult<(String, Value)> {
    let key = object_metadata
        .key()
        .ok_or_else(|| zerror!("Could not get key for object {:?}", object_metadata))?;
    let result = client.get_object(key).await;
    let output = result.map_err(|e| zerror!("Get operation failed: {e}"))?;
    Ok((key.to_string(), extract_value_from_response(output).await?))
}

/// Utility function to extract the [Value] from a result.
///
/// # Arguments
///
/// * `response`: response from the S3 server to a get object request.
async fn extract_value_from_response(response: GetObjectOutput) -> ZResult<Value> {
    let encoding = response.content_encoding().map(|x| x.to_string());
    let bytes = response
        .body
        .collect()
        .await
        .map(|data| data.into_bytes())?;

    let value = match encoding {
        Some(encoding) => Encoding::try_from(encoding).map_or_else(
            |_| Value::from(Vec::from(bytes.to_owned())),
            |result| Value::from(Vec::from(bytes.to_owned())).encoding(result),
        ),
        None => Value::from(Vec::from(bytes)),
    };
    Ok(value)
}

/// Utility function to reply to a query having a wild key. It is intended to be used by multiple
/// tasks running in parallel.
async fn reply_query(query: Arc<Query>, prefix: String, key: String, value: Value) -> ZResult<()> {
    Ok(query
        .reply(Sample::new(
            KeyExpr::try_from(prefix.to_owned() + "/" + key.as_str())?,
            value,
        ))
        .res()
        .await
        .map_err(|e| zerror!("{e}"))?)
}
