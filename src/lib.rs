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

pub mod client;
pub mod config;
pub mod utils;

use async_std::sync::Arc;
use async_trait::async_trait;

use client::S3Client;
use config::S3Config;
use utils::{S3Key, S3Value};

use futures::future::join_all;
use std::convert::TryFrom;

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

        let s3_key = S3Key::from_key_expr(
            Some(self.config.path_prefix.to_owned()),
            sample.key_expr.to_owned(),
        )?;

        match sample.kind {
            SampleKind::Put => {
                if !self.config.is_read_only {
                    let client2 = self.client.clone();
                    let sample2 = sample.to_owned();
                    let key2 = s3_key.into();
                    self.runtime
                        .spawn(async move { client2.put_object(key2, sample2).await })
                        .await
                        .map_err(|e| zerror!("Put operation failed: {e}"))?
                        .map_err(|e| zerror!("Put operation failed: {e}"))?;
                    Ok(StorageInsertionResult::Inserted)
                } else {
                    log::warn!("Received PUT for read-only DB on {} - ignored", s3_key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Delete => {
                if !self.config.is_read_only {
                    let client2 = self.client.clone();
                    let key2 = s3_key.into();

                    self.runtime
                        .spawn(async move { client2.delete_object(key2).await })
                        .await
                        .map_err(|e| zerror!("Delete operation failed: {e}"))?
                        .map_err(|e| zerror!("Delete operation failed: {e}"))?;
                    Ok(StorageInsertionResult::Deleted)
                } else {
                    log::warn!("Received DELETE for read-only DB on {} - ignored", s3_key);
                    Err("Received update for read-only DB".into())
                }
            }
        }
    }

    async fn on_query(&mut self, query: Query) -> ZResult<()> {
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
                .spawn(async move {
                    client
                        .get_intersecting_objects(&key_expr, Some(prefix))
                        .await
                })
                .await
                .map_err(|e| zerror!("Get operation failed: {e}"))?
                .map_err(|e| zerror!("Get operation failed: {e}"))?;

            join_all(
                intersecting_objects
                    .into_iter()
                    .map(|object| {
                        let client = self.client.clone();
                        let query = arc_query.clone();
                        let prefix = Some(self.config.path_prefix.to_owned()); //TODO, make prefix optional
                        self.runtime.spawn(async move {
                            let key = object.key().ok_or_else(|| {
                                zerror!("Could not get key for object {:?}", object)
                            })?;
                            let s3_key = S3Key::from_key(prefix, key.to_string());
                            let result = client.get_value_from_storage(s3_key).await;
                            match result {
                                Ok(s3_value) => S3Storage::reply_query(query, s3_value).await,
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
            let s3_key = S3Key::from_key_expr(
                Some(self.config.path_prefix.to_owned()),
                query.key_expr().to_owned(),
            )?;

            let value = self.get_stored_value(&s3_key.into()).await?;
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

impl S3Storage {
    async fn get_stored_value(&self, key: &String) -> ZResult<Value> {
        let client2 = self.client.clone();
        let key2 = key.to_owned();
        log::debug!("XXXX {}", key.to_owned());
        let output_result = self
            .runtime
            .spawn(async move { client2.get_object(key2.as_str()).await })
            .await
            .map_err(|e| zerror!("Get operation failed for key '{key}': {e}"))?
            .map_err(|e| zerror!("Get operation failed for key '{key}': {e}"))?;

        let encoding = output_result.content_encoding().map(|x| x.to_string());
        let bytes = output_result
            .body
            .collect()
            .await
            .map(|data| data.into_bytes())
            .map_err(|e| {
                zerror!("Get operation failed. Couldn't process retrieved contents: {e}")
            })?;

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
    async fn reply_query(query: Arc<Query>, s3_value: S3Value) -> ZResult<()> {
        Ok(query
            .reply(Sample::new(
                KeyExpr::try_from(s3_value.key)?,
                s3_value.value,
            ))
            .res()
            .await
            .map_err(|e| zerror!("{e}"))?)
    }
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
