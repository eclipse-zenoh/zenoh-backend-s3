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

use client::S3Client;
use std::convert::TryFrom;

use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::properties::Properties;
use zenoh::time::Timestamp;
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{StorageConfig, VolumeConfig};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_backend_traits::*;
use zenoh_core::{bail, zerror};

use crate::config::S3Config;

// Properties used by the Backend
pub const PROP_S3_ENDPOINT: &str = "url";

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

    let endpoint = match config.rest.get(PROP_S3_ENDPOINT) {
        Some(serde_json::Value::String(endpoint)) => endpoint.clone(),
        _ => {
            bail!(
                "Mandatory property `{}` for S3 Backend must be a string",
                PROP_S3_ENDPOINT
            )
        }
    };

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
    }))
}

pub struct S3Backend {
    admin_status: serde_json::Value,
    endpoint: String,
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
            config.region.to_owned(),
            config.bucket.to_owned(),
            self.endpoint.to_owned(),
        )
        .await;

        if config.create_bucket_is_enabled {
            let create_bucket_result = client.create_bucket();
            if create_bucket_result.is_err() {
                log::debug!("{}", create_bucket_result.unwrap_err().to_string());
            }
        }

        let storage_runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                log::debug!("Tokio runtime created for storage operations.");
                runtime
            }
            Err(err) => bail!(err),
        };

        Ok(Box::new(S3Storage {
            config,
            client,
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
    client: S3Client,
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
            "'{}' called on client {:?}. Key: '{}'",
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
                    self.runtime
                        .block_on(async {
                            self.client
                                .put_object(key.to_string(), sample.to_owned())
                                .await
                        })
                        .map_err(|e| zerror!("Put operation failed: {e}"))?;
                    Ok(StorageInsertionResult::Inserted)
                } else {
                    log::warn!("Received PUT for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Delete => {
                if !self.config.is_read_only {
                    match self
                        .runtime
                        .block_on(async { self.client.delete_object(key.to_string()).await })
                    {
                        Ok(_) => Ok(StorageInsertionResult::Deleted),
                        Err(err) => {
                            let error_msg = format!(
                                "Delete operation on bucket '{:?}' failed: {}",
                                self.client, err
                            );
                            Err(error_msg.into())
                        }
                    }
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
            "Query operation received for '{}' on bucket '{:?}'.",
            query.key_expr().as_str(),
            self.client
        );

        match self
            .runtime
            .block_on(async { self.client.get_object(key).await })
        {
            Ok(result) => {
                let encoding = result.content_encoding().map(|x| x.to_string());
                match result.body.collect().await.map(|data| data.into_bytes()) {
                    Ok(bytes) => {
                        let value = match encoding {
                            Some(encoding) => match Encoding::try_from(encoding) {
                                Ok(encoding) => Value::from(Vec::from(bytes)).encoding(encoding),
                                Err(_) => Value::from(Vec::from(bytes)),
                            },
                            None => Value::from(Vec::from(bytes)),
                        };
                        query
                            .reply(Sample::new(query.key_expr().clone(), value))
                            .res()
                            .await
                    }
                    Err(_) => Err("Failure parsing content".into()),
                }
            }
            Err(err) => {
                log::debug!(
                    "Query operation on bucket '{:?}' for key '{}' failed: '{}'",
                    self.client,
                    key,
                    err
                );
                Err(err)
            }
        }
    }

    // TODO(https://github.com/DariusIMP/zenoh-backend-s3/issues/1): create
    // mechanism to store the Timestamp id and time on the bucket files in
    // order to retrieve them here below.
    async fn get_all_entries(&self) -> ZResult<Vec<(OwnedKeyExpr, Timestamp)>> {
        todo!(
            "Issue 'https://github.com/DariusIMP/zenoh-backend-s3/issues/1' 
        needs to be solved first before being able to retrieve all the entries."
        );
    }
}

///
impl Drop for S3Storage {
    fn drop(&mut self) {
        async_std::task::block_on(async move {
            //TODO: task::spawn vs task::block_on
            match self.config.on_closure {
                config::OnClosure::DestroyBucket => match self.client.delete_bucket().await {
                    Ok(_) => log::debug!("Closing S3 storage {:?}", self.client),
                    Err(err) => log::debug!(
                        "Error while closing S3 storage {:?}: {}",
                        self.client,
                        err.to_string()
                    ),
                },
                config::OnClosure::DoNothing => {
                    log::debug!(
                        "Close S3 storage, keeping bucket '{:?}' as it is.",
                        self.client
                    );
                }
            }
        });
    }
}
