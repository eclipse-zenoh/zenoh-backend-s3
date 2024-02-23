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
use config::{S3Config, TlsClientConfig, TLS_PROP};
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use utils::S3Key;
use zenoh_plugin_trait::{plugin_version, Plugin};

use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::vec;

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

// Special key for None (when the prefix being stripped exactly matches the key)
pub const NONE_KEY: &str = "@@none_key@@";

// Metadata keys
pub const TIMESTAMP_METADATA_KEY: &str = "timestamp_uhlc";

// Amount of worker threads to be used by the tokio runtime of the [S3Storage] to handle incoming
// operations.
const STORAGE_WORKER_THREADS: usize = 2;

pub struct S3Backend {}
zenoh_plugin_trait::declare_plugin!(S3Backend);

impl Plugin for S3Backend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "s3_backend";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = zenoh_plugin_trait::plugin_long_version!();

    fn start(_name: &str, config: &Self::StartArgs) -> ZResult<Self::Instance> {
        // For some reasons env_logger is sometime not active in a loaded library.
        // Try to activate it here, ignoring failures.
        let _ = env_logger::try_init();
        log::debug!("S3 Backend {}", Self::PLUGIN_LONG_VERSION);

        let mut config = config.clone();
        config
            .rest
            .insert("version".into(), Self::PLUGIN_LONG_VERSION.into());

        let endpoint = get_optional_string_property(PROP_S3_ENDPOINT, &config)?;
        let region = get_optional_string_property(PROP_S3_REGION, &config)?;

        let mut properties = Properties::default();
        properties.insert("version".into(), Self::PLUGIN_LONG_VERSION.into());

        let admin_status = HashMap::from(properties)
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();

        let tls_config = load_tls_config(&config)?;

        Ok(Box::new(S3Volume {
            admin_status,
            endpoint,
            region,
            tls_config,
        }))
    }
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

fn load_tls_config(config: &VolumeConfig) -> ZResult<Option<TlsClientConfig>> {
    match config.rest.get(TLS_PROP) {
        Some(serde_json::Value::Object(tls_config)) => Ok(Some(TlsClientConfig::new(tls_config)?)),
        None => Ok(None),
        _ => Err(zerror!("Property {TLS_PROP} is malformed.").into()),
    }
}

pub struct S3Volume {
    admin_status: serde_json::Value,
    endpoint: Option<String>,
    region: Option<String>,
    tls_config: Option<TlsClientConfig>,
}

#[async_trait]
impl Volume for S3Volume {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    async fn create_storage(&self, config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        log::debug!("Creating storage...");
        let config: S3Config = S3Config::new(&config).await?;

        let client = S3Client::new(
            config.credentials.to_owned(),
            config.bucket.to_owned(),
            self.region.to_owned(),
            self.endpoint.to_owned(),
            self.tls_config.to_owned(),
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

    /// Returns the capability of this backend
    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Durable,
            history: History::Latest,
            read_cost: 1,
        }
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

    /// Function to retrieve the sample associated with a single key.
    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        let key = key.map_or_else(|| OwnedKeyExpr::from_str(NONE_KEY), Ok)?;
        log::debug!("GET called on client {}. Key: '{}'", self.client, key);

        let prefix = self.config.path_prefix.to_owned();
        let s3_key = S3Key::from_key_expr(prefix, key.to_owned())?;

        let get_result = self.get_stored_value(&s3_key.into()).await?;
        if let Some((timestamp, value)) = get_result {
            let stored_data = StoredData { value, timestamp };
            Ok(vec![stored_data])
        } else {
            Ok(vec![])
        }
    }

    /// Function called for each incoming data ([`Sample`]) to be stored in this storage.
    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        value: Value,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let key = key.map_or_else(|| OwnedKeyExpr::from_str(NONE_KEY), Ok)?;
        log::debug!("Put called on client {}. Key: '{}'", self.client, key);

        let s3_key = S3Key::from_key_expr(self.config.path_prefix.to_owned(), key)
            .map_or_else(|err| Err(zerror!("Error getting s3 key: {}", err)), Ok)?;
        if !self.config.is_read_only {
            let client2 = self.client.clone();
            let key2 = s3_key.into();
            let mut metadata: HashMap<String, String> = HashMap::new();
            metadata.insert(TIMESTAMP_METADATA_KEY.to_string(), timestamp.to_string());
            self.runtime
                .spawn(async move { client2.put_object(key2, value, Some(metadata)).await })
                .await
                .map_err(|e| zerror!("Put operation failed: {e}"))?
                .map_err(|e| zerror!("Put operation failed: {e}"))?;
            Ok(StorageInsertionResult::Inserted)
        } else {
            log::warn!("Received PUT for read-only DB on {} - ignored", s3_key);
            Err("Received update for read-only DB".into())
        }
    }

    /// Function called for each incoming delete request to this storage.
    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let key = key.map_or_else(|| OwnedKeyExpr::from_str(NONE_KEY), Ok)?;
        log::debug!("Delete called on client {}. Key: '{}'", self.client, key);

        let s3_key = S3Key::from_key_expr(self.config.path_prefix.to_owned(), key)?;
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

    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let client = self.client.clone();
        let objects = self
            .runtime
            .spawn(async move { client.list_objects_in_bucket().await })
            .await
            .map_err(|e| zerror!("Get operation failed: {e}"))?
            .map_err(|e| zerror!("Get operation failed: {e}"))?;

        let futures = objects.into_iter().map(|object| {
            let client = self.client.clone();
            self.runtime.spawn(async move {
                let key = object
                    .key()
                    .ok_or_else(|| zerror!("Could not get key for object {:?}", object))?;
                let result = client.get_head_object(key).await;
                match result {
                    Ok(value) => {
                        let key_expr = if key == NONE_KEY {
                            None
                        } else {
                            Some(OwnedKeyExpr::try_from(key).map_err(|err| {
                                zerror!("Unable to recreate key expression for '{}': {}.", key, err)
                            })?)
                        };
                        let metadata = value.metadata.ok_or_else(|| {
                            zerror!("Unable to retrieve metadata for key '{}'.", key)
                        })?;
                        let timestamp = metadata.get(TIMESTAMP_METADATA_KEY).ok_or_else(|| {
                            zerror!("Unable to retrieve timestamp for key '{}'.", key)
                        })?;
                        Ok((
                            key_expr,
                            Timestamp::from_str(timestamp.as_str()).map_err(|e| {
                                zerror!("Unable to obtain timestamp for key: {}. {:?}", key, e)
                            })?,
                        ))
                    }
                    Err(err) => Err(zerror!(
                        "Unable to get '{}' object from storage: {}",
                        key.to_owned(),
                        err
                    )),
                }
            })
        });
        let futures_results = join_all(futures.collect::<FuturesUnordered<_>>()).await;
        let entries: Vec<(Option<OwnedKeyExpr>, Timestamp)> =
            futures_results.into_iter().flatten().flatten().collect();
        Ok(entries)
    }
}

impl S3Storage {
    async fn get_stored_value(&self, key: &String) -> ZResult<Option<(Timestamp, Value)>> {
        let client2 = self.client.clone();
        let key2 = key.to_owned();

        let output_result = match self
            .runtime
            .spawn(async move { client2.get_object(key2.as_str()).await })
            .await
            .map_err(|e| zerror!("Get operation failed for key '{key}': {e}"))?
        {
            Ok(result) => Ok(result),
            Err(e) => {
                if e.to_string().contains("NoSuchKey") {
                    return Ok(None);
                }
                Err(zerror!("Get operation failed for key '{key}': {e}"))
            }
        }?;

        let metadata = output_result
            .metadata
            .as_ref()
            .ok_or_else(|| zerror!("Unable to retrieve metadata."))?;
        let timestamp = metadata
            .get(TIMESTAMP_METADATA_KEY)
            .ok_or_else(|| zerror!("Unable to retrieve timestamp."))?;
        let timestamp = Timestamp::from_str(timestamp.as_str())
            .map_err(|e| zerror!("Unable to obtain timestamp for key: {}. {:?}", key, e))?;

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
        Ok(Some((timestamp, value)))
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
