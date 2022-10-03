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

use async_std::sync::Arc;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::error::{DeleteObjectsError, ListObjectsV2Error, PutObjectError};
use aws_sdk_s3::model::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, Object, ObjectIdentifier,
};
use aws_sdk_s3::output::{DeleteObjectsOutput, PutObjectOutput};
use aws_sdk_s3::types::{ByteStream, SdkError};
use aws_sdk_s3::{self, Client, Endpoint, Region};
use aws_smithy_http::operation::Response;
use aws_types::Credentials;
use log::{debug, warn};
use std::io::{Error, ErrorKind};

use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::properties::Properties;
use zenoh::time::Timestamp;
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{
    PrivacyGetResult, PrivacyTransparentGet, StorageConfig, VolumeConfig,
};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_backend_traits::*;
use zenoh_core::{bail, zerror};

// Properties used by the Backend
pub const PROP_S3_ACCESS_KEY: &str = "access_key";
pub const PROP_S3_BUCKET: &str = "bucket";
pub const PROP_S3_ENDPOINT: &str = "url";
pub const PROP_S3_REGION: &str = "region";
pub const PROP_S3_SECRET_KEY: &str = "secret_key";
pub const PROP_S3_SESSION_TOKEN: &str = "session_token";
pub const PROP_S3_PROVIDER: &str = "provider";

// Properties used by the Storage
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_CREATE_BUCKET: &str = "create_bucket";
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";
pub const PROP_STRIP_PREFIX: &str = "strip_prefix";

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static! {
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
}

// Constants
const DEFAULT_PROVIDER: &str = "zenoh-s3-backend";

pub(crate) enum OnClosure {
    DestroyBucket,
    DoNothing,
}

#[allow(dead_code)]
const CREATE_BACKEND_TYPECHECK: CreateVolume = create_volume;

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

        let credentials = match _load_credentials_config(&config) {
            Ok(credentials) => credentials,
            Err(err) => bail!(err),
        };

        let region = match _load_region_config(&config).await {
            Ok(region) => region,
            Err(err) => bail!(err),
        };

        let bucket = match _load_bucket_name_config(&config) {
            Ok(name) => name,
            Err(err) => bail!(err),
        };

        let path_prefix = match _load_path_prefix_config(&config) {
            Ok(prefix) => prefix,
            Err(err) => bail!(err),
        };

        let read_only = match _is_read_only_config(&config) {
            Ok(read_only) => read_only,
            Err(err) => bail!(err),
        };

        let on_closure = match _load_on_closure_config(&config) {
            Ok(on_closure) => on_closure,
            Err(err) => bail!(err),
        };

        let sdk_config = aws_config::ConfigLoader::default()
            .endpoint_resolver(Endpoint::immutable(
                self.endpoint.parse().expect("Invalid endpoint: "),
            ))
            .region(region.to_owned())
            .credentials_provider(credentials)
            .load()
            .await;

        let client = Client::new(&sdk_config);

        let create_bucket_config = _load_create_bucket_config(&config);

        if create_bucket_config {
            create_bucket(client.to_owned(), bucket.to_string(), region.to_string());
        }

        let storage_runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                debug!("Tokio runtime created for storage operations.");
                runtime
            }
            Err(err) => bail!(err),
        };

        Ok(Box::new(S3Storage {
            admin_status: config,
            runtime: storage_runtime,
            path_prefix,
            on_closure,
            read_only,
            client,
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
    runtime: tokio::runtime::Runtime,
    path_prefix: String,
    on_closure: OnClosure,
    read_only: bool,
    client: Client,
    bucket: String,
}

#[async_trait]
impl Storage for S3Storage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.to_json_value()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<StorageInsertionResult> {
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

        debug!("'{}' called. Key: '{}'", sample.kind, key);

        match sample.kind {
            SampleKind::Put => {
                if !self.read_only {
                    // encode the value as a string to be stored in S3, converting to base64 if the buffer is not a UTF-8 string
                    let (_, strvalue) =
                        match String::from_utf8(sample.payload.contiguous().into_owned()) {
                            Ok(s) => (false, s),
                            Err(err) => (true, base64::encode(err.into_bytes())),
                        };

                    match self.runtime.block_on(async {
                        put_object(
                            &self.client,
                            self.bucket.to_owned(),
                            key.to_string(),
                            strvalue,
                        )
                        .await
                    }) {
                        Ok(_) => Ok(StorageInsertionResult::Inserted),
                        Err(err) => {
                            let error_msg = format!(
                                "Put operation on bucket '{}' failed: {}",
                                self.bucket,
                                err.to_string()
                            );
                            Err(error_msg.into())
                        }
                    }
                } else {
                    warn!("Received PUT for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Delete => {
                if !self.read_only {
                    match self.runtime.block_on(async {
                        self.client
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(key)
                            .send()
                            .await
                    }) {
                        Ok(_) => Ok(StorageInsertionResult::Deleted),
                        Err(err) => {
                            let error_msg = format!(
                                "Delete operation on bucket '{}' failed: {}",
                                self.bucket,
                                err.to_string()
                            );
                            Err(error_msg.into())
                        }
                    }
                } else {
                    warn!("Received DELETE for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
        }
    }

    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        let key = query
            .key_expr()
            .as_str()
            .strip_prefix(&self.path_prefix)
            .ok_or_else(|| {
                zerror!(
                    "Received a Query not starting with path_prefix '{}'",
                    self.path_prefix
                )
            })?;

        debug!(
            "Query operation received for '{}' on bucket '{}'.",
            query.key_expr().as_str(),
            self.bucket
        );

        match self.runtime.block_on(async {
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
        }) {
            Ok(result) => {
                match result.body.collect().await.map(|data| data.into_bytes()) {
                    Ok(bytes) => {
                        // TODO(https://github.com/DariusIMP/zenoh-backend-s3/issues/2): manage content types
                        let data = String::from_utf8_lossy(&Vec::from(bytes)).to_string();
                        query
                            .reply(Sample::new(query.key_expr().clone(), Value::from(data)))
                            .res()
                            .await
                    }
                    Err(_) => Err("Failure parsing content".into()),
                }
            }
            Err(err) => {
                let error_msg = format!(
                    "Query operation on bucket '{}' for key '{}' failed: '{}'",
                    self.bucket,
                    key,
                    err.to_string()
                );
                Err(error_msg.into())
            }
        }
    }

    // TODO(https://github.com/DariusIMP/zenoh-backend-s3/issues/1): create
    // mechanism to store the Timestamp id and time on the bucket files in
    // order to retrieve them here below.
    async fn get_all_entries(&self) -> ZResult<Vec<(OwnedKeyExpr, Timestamp)>> {
        debug!("Get all entries.");
        todo!(
            "Issue 'https://github.com/DariusIMP/zenoh-backend-s3/issues/1' 
        needs to be solved first before being able to retrieve all the entries."
        );
    }
}

impl Drop for S3Storage {
    fn drop(&mut self) {
        async_std::task::block_on(async move {
            match self.on_closure {
                OnClosure::DestroyBucket => {
                    debug!("Close S3 storage, destroying bucket '{}'.", self.bucket);
                    self.runtime.block_on(async {
                        let objects = _list_objects_in_bucket(&self.client, &self.bucket).await;
                        if objects.is_err() {
                            debug!("Failed to destroy bucket '{}', unable to retrieve contained files for prior removal: {:#?}", self.bucket.to_owned(), objects.unwrap_err());
                            return;
                        }
                        let delete_objects_result = _delete_objects_in_bucket(&self.client, &self.bucket, objects.unwrap_or_default()).await;
                        if delete_objects_result.is_err() {
                            debug!("Failed to destroy bucket '{}', unable to empty the bucket before removing it: {:#?}", self.bucket.to_owned(), delete_objects_result.unwrap_err());
                            return;
                        }
                        debug!("Deleted objects from bucket '{}': {:#?}", self.bucket.to_owned(), delete_objects_result.unwrap());
                        let delete_bucket_result = self.client.delete_bucket().bucket(&self.bucket).send().await;
                        if delete_bucket_result.is_err() {
                            debug!("Failed to destroy bucket '{}': {:?}", self.bucket.to_owned(), delete_bucket_result.unwrap_err());
                            return;
                        }
                        debug!("Deleted bucket '{}'.", self.bucket.to_owned());
                    });
                }
                OnClosure::DoNothing => {
                    debug!(
                        "Close S3 storage, keeping bucket '{}' as it is.",
                        self.bucket
                    );
                }
            }
        });
    }
}

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

#[tokio::main]
async fn create_bucket(client: Client, bucket: String, region: String) {
    let constraint = BucketLocationConstraint::from(region.as_str());
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(constraint)
        .build();
    let creation_result = client
        .create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket.to_owned())
        .send()
        .await;
    match creation_result {
        Ok(_) => debug!("Created bucket '{}'", bucket),
        Err(err) => debug!("Failure creating bucket '{}': {}", bucket, err.to_string()),
    }
}

/// Puts
///
async fn put_object(
    client: &Client,
    bucket: String,
    key: String,
    value: String,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let body = ByteStream::from(value.as_bytes().to_vec());
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await
}

fn _load_create_bucket_config(config: &StorageConfig) -> bool {
    match config.volume_cfg.get(PROP_STORAGE_CREATE_BUCKET) {
        Some(serde_json::value::Value::Bool(value)) => value.to_owned(),
        _ => false,
    }
}

fn _load_on_closure_config(config: &StorageConfig) -> Result<OnClosure, Error> {
    match config.volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
        Some(serde_json::Value::String(s)) if s == "destroy_bucket" => Ok(OnClosure::DestroyBucket),
        Some(serde_json::Value::String(s)) if s == "do_nothing" => Ok(OnClosure::DoNothing),
        None => Ok(OnClosure::DoNothing),
        _ => {
            let error_msg = format!(
                r#"Optional property `{}` of S3 storage configurations must be either "do_nothing" (default) or "destroy_bucket""#,
                PROP_STORAGE_ON_CLOSURE
            );
            Err(Error::new(ErrorKind::InvalidInput, error_msg))
        }
    }
}

fn _is_read_only_config(config: &StorageConfig) -> Result<bool, Error> {
    match config.volume_cfg.get(PROP_STORAGE_READ_ONLY) {
        None | Some(serde_json::Value::Bool(false)) => Ok(false),
        Some(serde_json::Value::Bool(true)) => Ok(true),
        _ => {
            let error_msg = format!(
                "Optional property `{}` of s3 storage configurations must be a boolean",
                PROP_STORAGE_READ_ONLY
            );
            Err(Error::new(ErrorKind::InvalidInput, error_msg))
        }
    }
}

fn _load_path_prefix_config(config: &StorageConfig) -> Result<String, Error> {
    let path_expr = config.key_expr.to_owned();

    let path_prefix = match config.strip_prefix.to_owned() {
        Some(prefix) => Ok(prefix.to_string()),
        None => {
            let error_msg = format!(
                "Property '{PROP_STRIP_PREFIX}' was not specified on the configuration file!"
            );
            Err(Error::new(ErrorKind::InvalidInput, error_msg))
        }
    };

    match path_prefix {
        Ok(prefix) => {
            if !path_expr.starts_with(&prefix) {
                let error_msg = format!(
                    r#"The specified "strip_prefix={}" is not a prefix of "key_expr={}""#,
                    prefix, path_expr
                );
                return Err(Error::new(ErrorKind::InvalidInput, error_msg));
            } else {
                return Ok(prefix);
            }
        }
        Err(err) => Err(err),
    }
}

fn _load_bucket_name_config(config: &StorageConfig) -> Result<String, Error> {
    let bucket_name = match config.volume_cfg.get(PROP_S3_BUCKET) {
        Some(serde_json::Value::String(name)) => Ok(name.to_owned()),
        _ => {
            let error_msg = format!("Property '{PROP_S3_BUCKET}' was not specified!"); //TODO: handle unspecified bucket name
            Err(Error::new(ErrorKind::InvalidInput, error_msg))
        }
    };
    return bucket_name;
}

/// Loads the credentials from the configuration json file.
///
/// TODO: fill comment.
fn _load_credentials_config(config: &StorageConfig) -> Result<Credentials, Error> {
    let volume_cfg = match config.volume_cfg.as_object() {
        Some(v) => v,
        None => {
            let error_msg =
                "Couldn't retrieve private properties of the storage from json5 config file.";
            return Err(Error::new(ErrorKind::InvalidInput, error_msg));
        },
    };

    let access_key = match get_private_conf(volume_cfg, PROP_S3_ACCESS_KEY) {
        Ok(access_key) => access_key,
        Err(err) => {
            let error_msg = format!("Error loading property '{PROP_S3_ACCESS_KEY}': {}", err.to_string());
            return Err(Error::new(ErrorKind::InvalidInput, error_msg));
        },
    };

    let secret_key = match get_private_conf(volume_cfg, PROP_S3_SECRET_KEY) {
        Ok(secret_key) => secret_key,
        Err(err) => {
            let error_msg = format!("Error loading property '{PROP_S3_SECRET_KEY}': {}", err.to_string());
            return Err(Error::new(ErrorKind::InvalidInput, error_msg));
        },
    };

    if !access_key.is_some() {
        let error_msg = format!("Property '{PROP_S3_ACCESS_KEY}' needs to be of specified!");
        return Err(Error::new(ErrorKind::InvalidInput, error_msg));
    }
    let access_key = access_key.unwrap();

    if !secret_key.is_some() {
        let error_msg = format!("Property '{PROP_S3_SECRET_KEY}' needs to be of specified!");
        return Err(Error::new(ErrorKind::InvalidInput, error_msg));
    }
    let secret_key = secret_key.unwrap();

    return Ok(Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        DEFAULT_PROVIDER,
    ));
}

/// Loads the region from the config if specified, returns None otherwise.
async fn _load_region_config(config: &StorageConfig) -> Result<Region, Error> {
    let region_value: Result<String, Error> = match config.volume_cfg.get(PROP_S3_REGION) {
        Some(value) => {
            if value.is_string() {
                Ok(value.as_str().map(|x| x.to_string()).unwrap())
            } else {
                let error_msg = format!("Property '{}' must be a string such as 'eu-west-3' or 'us-east-1', following the AWS specification.", PROP_S3_REGION);
                return Err(Error::new(ErrorKind::InvalidInput, error_msg));
            }
        }
        _ => {
            let error_msg =
                format!("Property '{PROP_S3_REGION}' was not specified on the configuration file!");
            return Err(Error::new(ErrorKind::InvalidInput, error_msg));
        }
    };
    match region_value {
        Ok(value) => {
            let region = RegionProviderChain::first_try(aws_sdk_s3::Region::new(value.to_owned()))
                .region()
                .await;
            match region {
                Some(region) => Ok(region),
                None => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unable to load storage region '{}'", value),
                )),
            }
        }
        Err(err) => Err(err),
    }
}

async fn _list_objects_in_bucket(
    client: &Client,
    bucket: &String,
) -> Result<Vec<Object>, SdkError<ListObjectsV2Error>> {
    match client.list_objects_v2().bucket(bucket).send().await {
        Ok(response) => Ok(response.contents().unwrap_or_default().to_vec()),
        Err(err) => Err(err),
    }
}

async fn _delete_objects_in_bucket(
    client: &Client,
    bucket: &String,
    objects: Vec<Object>,
) -> Result<DeleteObjectsOutput, SdkError<DeleteObjectsError, Response>> {
    if objects.is_empty() {
        return Ok(DeleteObjectsOutput::builder()
            .set_deleted(Some(vec![]))
            .build());
    }

    let mut object_identifiers: Vec<ObjectIdentifier> = vec![];

    for object in objects {
        let identifier = ObjectIdentifier::builder()
            .set_key(object.key().map(|x| x.to_string()))
            .build();
        object_identifiers.push(identifier);
    }

    let delete = Delete::builder()
        .set_objects(Some(object_identifiers))
        .build();

    return client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await;
}
