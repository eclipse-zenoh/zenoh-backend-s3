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

#![allow(unused)] // TODO(@darius): remove

use async_std::sync::{Arc, Mutex};
use async_std::task::{sleep, JoinHandle};
use async_trait::async_trait;
use http::status::InvalidStatusCode;
use http::{status, StatusCode};
use log::{debug, warn};
use s3::bucket::{self, Bucket};
use s3::creds::Credentials;
use s3::region::Region;
use s3::BucketConfiguration;
use std::fmt::Error;
use std::path::PathBuf;
use std::time::Duration;
use uhlc::NTP64;
use zenoh::prelude::*;
use zenoh::properties::Properties;
use zenoh::time::{new_reception_timestamp, Timestamp};
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{
    PrivacyGetResult, PrivacyTransparentGet, StorageConfig, VolumeConfig,
};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_backend_traits::*;
use zenoh_collections::Timer;
use zenoh_core::{bail, zerror};
use zenoh_util::zenoh_home;

// Properies used by the Backend
pub const PROP_S3_ENDPOINT: &str = "url";
pub const PROP_S3_ACCESS_KEY: &str = "access_key";
pub const PROP_S3_SECRET_KEY: &str = "secret_key";
pub const PROP_S3_REGION: &str = "region";
pub const PROP_S3_BUCKET: &str = "bucket";

// Properies used by the Backend
//  - None

// Properies used by the Storage
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_CREATE_DB: &str = "create_db";
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static! {
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
}

pub(crate) enum OnClosure {
    DestroyBucket,
    DoNothing,
}

#[allow(dead_code)]
const CREATE_BACKEND_TYPECHECK: CreateVolume = create_volume;

fn get_private_conf<'a>(
    config: &'a serde_json::Map<String, serde_json::Value>,
    credit: &str,
) -> ZResult<Option<&'a String>> {
    match config.get_private(credit) {
        PrivacyGetResult::NotFound => Ok(None),
        PrivacyGetResult::Private(serde_json::Value::String(v)) => Ok(Some(v)),
        PrivacyGetResult::Public(serde_json::Value::String(v)) => {
            log::warn!(
                r#"Value "{}" is given for `{}` publicly (i.e. is visible by anyone who can fetch the router configuration). You may want to replace `{}: "{}"` with `private: {{{}: "{}"}}`"#,
                v,
                credit,
                credit,
                v,
                credit,
                v
            );
            Ok(Some(v))
        }
        PrivacyGetResult::Both {
            public: serde_json::Value::String(public),
            private: serde_json::Value::String(private),
        } => {
            log::warn!(
                r#"Value "{}" is given for `{}` publicly, but a private value also exists. The private value will be used, but the public value, which is {} the same as the private one, will still be visible in configurations."#,
                public,
                credit,
                if public == private { "" } else { "not " }
            );
            Ok(Some(private))
        }
        _ => {
            bail!("Optional property `{}` must be a string", credit)
        }
    }
}

#[no_mangle]
pub fn create_volume(mut config: VolumeConfig) -> ZResult<Box<dyn Volume>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();
    debug!("S3 Backend {}", LONG_VERSION.as_str());

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
        debug!("Creating storage. ");
        let private_values = match (config.volume_cfg.get("private")) {
            Some(private_fiels) => private_fiels,
            _ => {
                bail!("Couldn't retrieve private properties of the storage from json5 config file.")
            }
        };

        let access_key = private_values.get(PROP_S3_ACCESS_KEY);
        let secret_key = private_values.get(PROP_S3_SECRET_KEY);

        let credentials = match (access_key, secret_key) {
            (Some(access_key), Some(secret_key)) => Credentials {
                access_key: Some(access_key.to_string()),
                secret_key: Some(secret_key.to_string()),
                security_token: None,
                session_token: None,
            },
            _ => {
                bail!(
                    "Optional properties `{}` and `{}` must coexist",
                    PROP_S3_ACCESS_KEY,
                    PROP_S3_SECRET_KEY
                )
            }
        };

        let region = match (config.volume_cfg.get(PROP_S3_REGION)) {
            Some(serde_json::Value::String(region)) => Region::Custom {
                region: region.to_owned(),
                endpoint: self.endpoint.to_string(),
            },
            _ => {
                warn!("Property {PROP_S3_REGION} was not specified!"); //TODO: handle unspecified region
                Region::Custom {
                    region: "".to_owned(),
                    endpoint: self.endpoint.to_string(),
                }
            }
        };

        let bucket_name = match (config.volume_cfg.get(PROP_S3_BUCKET)) {
            Some(serde_json::Value::String(name)) => name,
            _ => {
                warn!("Property {PROP_S3_BUCKET} was not specified!"); //TODO: handle unspecified bucket name
                "zenoh-s3"
            }
        };

        let bucket = Bucket::new_with_path_style(bucket_name, region, credentials)?;

        let path_expr = config.key_expr.clone();
        let path_prefix = config.strip_prefix.clone().unwrap().to_string();
        if !path_expr.starts_with(&path_prefix) {
            bail!(
                r#"The specified "strip_prefix={}" is not a prefix of "key_expr={}""#,
                path_prefix,
                path_expr
            )
        }

        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("S3 backend storages need volume-specific configurations"), //TODO: check
        };

        let read_only = match volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => false,
            Some(serde_json::Value::Bool(true)) => true,
            _ => {
                bail!(
                    "Optional property `{}` of s3 storage configurations must be a boolean",
                    PROP_STORAGE_READ_ONLY
                )
            }
        };

        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_bucket" => OnClosure::DestroyBucket,
            Some(serde_json::Value::String(s)) if s == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            _ => {
                bail!(
                    r#"Optional property `{}` of S3 storage configurations must be either "do_nothing" (default) or "destroy_bucket""#,
                    PROP_STORAGE_ON_CLOSURE
                )
            }
        };

        Ok(Box::new(S3Storage {
            admin_status: config,
            path_prefix,
            on_closure,
            read_only,
            bucket,
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
    admin_status: StorageConfig,
    path_prefix: String,
    on_closure: OnClosure,
    read_only: bool,
    bucket: Bucket,
}

#[async_trait]
impl Storage for S3Storage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.to_json_value()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<StorageInsertionResult> {
        debug!("On sample called.");
        let key = sample
            .key_expr
            .as_str()
            .strip_prefix(&self.path_prefix)
            .ok_or_else(|| {
                zerror!(
                    "Received a Sample not starting with path_prefix '{}'",
                    self.path_prefix
                )
            })?;

        // get latest timestamp for this key (if already exists in db)
        // and drop incoming sample if older
        let sample_ts = sample.timestamp.unwrap_or_else(new_reception_timestamp);

        match sample.kind {
            SampleKind::Put => {
                if !self.read_only {
                    put_kv(&self.bucket, &key, sample.value, sample_ts).await
                } else {
                    warn!("Received PUT for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Delete => todo!(),
        }
    }

    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        debug!("On query called.");
        todo!();
    }

    async fn get_all_entries(&self) -> ZResult<Vec<(OwnedKeyExpr, Timestamp)>> {
        debug!("Get all entries.");
        todo!();
    }
}

async fn put_kv(
    bucket: &Bucket,
    key: &str,
    value: Value,
    sample_ts: Timestamp,
) -> ZResult<StorageInsertionResult> {
    //@TODO: Refactor code
    let result = bucket
        .put_object_with_content_type(key.to_string(), value.to_string().as_bytes(), "text/plain")
        .await;
    match result {
        Ok((_, status_code)) => match StatusCode::from_u16(status_code) {
            Ok(status_code) => {
                debug!("Put success");
                return Ok(StorageInsertionResult::Inserted);
            }
            Err(_) => {
                debug!("Put failure");
                return Err(zerror!(
                    "S3 PUT operation failure: invalid status code {}.",
                    status_code
                )
                .into());
            }
        },
        Err(e) => {
            debug!("Put failure");
            return Err(zerror!("S3 PUT operation failure: {}", e.to_string()).into());
        }
    };
}

fn s3_err_to_zerr(status_code: StatusCode) -> zenoh_core::Error {
    zerror!("S3 operation failure: {}", status_code.as_str()).into()
}

impl Drop for S3Storage {
    fn drop(&mut self) {
        async_std::task::block_on(async move {
            match self.on_closure {
                OnClosure::DestroyBucket => {
                    debug!("Close S3 storage, destroying bucket {}", self.path_prefix);
                    todo!();
                }
                OnClosure::DoNothing => {
                    debug!(
                        "Close S3 storage, keeping database {} as it is",
                        self.path_prefix
                    );
                }
            }
        });
    }
}
