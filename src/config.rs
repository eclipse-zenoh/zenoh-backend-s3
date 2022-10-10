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

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Credentials, Region};
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{PrivacyGetResult, PrivacyTransparentGet, StorageConfig};
use zenoh_core::{bail, zerror};

// Properties used by the Backend
const PROP_S3_ACCESS_KEY: &str = "access_key";
const PROP_S3_BUCKET: &str = "bucket";
const PROP_S3_REGION: &str = "region";
const PROP_S3_SECRET_KEY: &str = "secret_key";

// Properties used by the Storage
const PROP_STORAGE_CREATE_BUCKET: &str = "create_bucket";
const PROP_STORAGE_READ_ONLY: &str = "read_only";
const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";
const PROP_STRIP_PREFIX: &str = "strip_prefix";

const DEFAULT_PROVIDER: &str = "zenoh-s3-backend";

pub enum OnClosure {
    DestroyBucket,
    DoNothing,
}

pub struct S3Config {
    pub credentials: Credentials,
    pub region: Region,
    pub bucket: String,
    pub path_prefix: String,
    pub is_read_only: bool,
    pub on_closure: OnClosure,
    pub admin_status: serde_json::Value,
    pub create_bucket_is_enabled: bool,
}

impl S3Config {
    pub async fn new(config: &StorageConfig) -> ZResult<Self> {
        let credentials = S3Config::load_credentials(config)?;
        let region = S3Config::load_region(config).await?;
        let path_prefix = S3Config::load_path_prefix(config)?;
        let bucket = S3Config::load_bucket_name(config)?;
        let is_read_only = S3Config::is_read_only(config)?;
        let on_closure = S3Config::load_on_closure(config)?;
        let create_bucket_is_enabled = S3Config::create_bucket_is_enabled(config);
        let admin_status = config.to_json_value();

        Ok(S3Config {
            credentials,
            region,
            bucket,
            path_prefix,
            is_read_only,
            on_closure,
            admin_status,
            create_bucket_is_enabled,
        })
    }

    fn load_credentials(config: &StorageConfig) -> ZResult<Credentials> {
        let volume_cfg = config.volume_cfg.as_object().map_or_else(
            || Err("Couldn't retrieve private properties of the storage from json5 config file."),
            Ok,
        )?;

        let access_key = get_private_conf(volume_cfg, PROP_S3_ACCESS_KEY)
            .map_err(|err| zerror!("Could not load '{}': {}", PROP_S3_ACCESS_KEY, err))?
            .map_or_else(
                || {
                    Err(zerror!(
                        "Property '{PROP_S3_ACCESS_KEY}' needs to be of specified!"
                    ))
                },
                Ok,
            )?;

        let secret_key = get_private_conf(volume_cfg, PROP_S3_SECRET_KEY)?.map_or_else(
            || {
                Err(zerror!(
                    "Property '{PROP_S3_SECRET_KEY}' needs to be of specified!"
                ))
            },
            Ok,
        )?;

        Ok(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            DEFAULT_PROVIDER,
        ))
    }

    async fn load_region(config: &StorageConfig) -> ZResult<Region> {
        let region_code = config
            .volume_cfg
            .get(PROP_S3_REGION)
            .map_or_else(
                || {
                    Err(zerror!(
                        "Property '{PROP_S3_REGION}' was not specified on the configuration file!"
                    ))
                },
                |region| Ok(region.to_string()),
            )
            .map_err(|err| zerror!("Unable to load storage region: {err}"))?;

        let region =
            RegionProviderChain::first_try(aws_sdk_s3::Region::new(region_code.to_owned()))
                .region()
                .await
                .map_or_else(
                    || Err(zerror!("Unable to load storage region '{region_code}'")),
                    Ok,
                )?;
        Ok(region)
    }

    fn load_bucket_name(config: &StorageConfig) -> ZResult<String> {
        Ok(match config.volume_cfg.get(PROP_S3_BUCKET) {
            Some(serde_json::Value::String(name)) => Ok(name.to_owned()),
            _ => Err(zerror!("Property '{PROP_S3_BUCKET}' was not specified!")),
        }?)
    }

    fn load_path_prefix(config: &StorageConfig) -> ZResult<String> {
        let path_expr = config.key_expr.to_owned();
        let prefix = config.strip_prefix.to_owned().map_or_else(
            || {
                Err(zerror!(
                    "Property '{PROP_STRIP_PREFIX}' was not specified on the configuration file!"
                ))
            },
            |prefix| Ok(prefix.to_string()),
        )?;
        if !path_expr.starts_with(&prefix) {
            Err(zerror!(
                r#"The specified "strip_prefix={}" is not a prefix of "key_expr={}""#,
                prefix,
                path_expr
            )
            .into())
        } else {
            Ok(prefix)
        }
    }

    fn is_read_only(config: &StorageConfig) -> ZResult<bool> {
        match config.volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => Ok(false),
            Some(serde_json::Value::Bool(true)) => Ok(true),
            _ => Err(zerror!("Optional property `{PROP_STORAGE_READ_ONLY}` of s3 storage configurations must be a boolean").into())
        }
    }

    fn load_on_closure(config: &StorageConfig) -> ZResult<OnClosure> {
        match config.volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_bucket" => Ok(OnClosure::DestroyBucket),
            Some(serde_json::Value::String(s)) if s == "do_nothing" => Ok(OnClosure::DoNothing),
            None => Ok(OnClosure::DoNothing),
            _ => Err(zerror!(r#"Optional property `{PROP_STORAGE_ON_CLOSURE}` of S3 storage configurations must be either "do_nothing" (default) or "destroy_bucket""#).into())
        }
    }

    fn create_bucket_is_enabled(config: &StorageConfig) -> bool {
        match config.volume_cfg.get(PROP_STORAGE_CREATE_BUCKET) {
            Some(serde_json::value::Value::Bool(value)) => value.to_owned(),
            _ => false,
        }
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
