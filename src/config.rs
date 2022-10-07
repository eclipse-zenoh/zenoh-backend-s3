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
    config: StorageConfig,
}

impl S3Config {
    pub fn new(config: StorageConfig) -> Self {
        S3Config { config }
    }

    pub fn get_admin_status(&self) -> serde_json::Value {
        self.config.to_json_value()
    }

    pub fn load_credentials(&self) -> ZResult<Credentials> {
        let volume_cfg = self.config.volume_cfg.as_object().map_or_else(
            || Err("Couldn't retrieve private properties of the storage from json5 config file."),
            |config| Ok(config),
        )?;

        let access_key = get_private_conf(volume_cfg, PROP_S3_ACCESS_KEY)
            .map_err(|err| zerror!("Could not load '{}': {}", PROP_S3_ACCESS_KEY, err))?
            .map_or_else(
                || {
                    Err(zerror!(
                        "Property '{PROP_S3_ACCESS_KEY}' needs to be of specified!"
                    ))
                },
                |key| Ok(key),
            )?;

        let secret_key = get_private_conf(volume_cfg, PROP_S3_SECRET_KEY)
            .map_err(|err| err)?
            .map_or_else(
                || {
                    Err(zerror!(
                        "Property '{PROP_S3_SECRET_KEY}' needs to be of specified!"
                    ))
                },
                |key| Ok(key),
            )?;

        return Ok(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            DEFAULT_PROVIDER,
        ));
    }

    pub async fn load_region(&self) -> ZResult<Region> {
        let region_code = self
            .config
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
                    |region| Ok(region),
                )?;
        Ok(region)
    }

    pub fn load_bucket_name(&self) -> ZResult<String> {
        Ok(match self.config.volume_cfg.get(PROP_S3_BUCKET) {
            Some(serde_json::Value::String(name)) => Ok(name.to_owned()),
            _ => Err(zerror!("Property '{PROP_S3_BUCKET}' was not specified!")),
        }?)
    }

    pub fn load_path_prefix(&self) -> ZResult<String> {
        let path_expr = self.config.key_expr.to_owned();
        let prefix = self.config.strip_prefix.to_owned().map_or_else(
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

    pub fn is_read_only(&self) -> ZResult<bool> {
        match self.config.volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => Ok(false),
            Some(serde_json::Value::Bool(true)) => Ok(true),
            _ => Err(zerror!("Optional property `{PROP_STORAGE_READ_ONLY}` of s3 storage configurations must be a boolean").into())
        }
    }

    pub fn load_on_closure(&self) -> ZResult<OnClosure> {
        match self.config.volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_bucket" => Ok(OnClosure::DestroyBucket),
            Some(serde_json::Value::String(s)) if s == "do_nothing" => Ok(OnClosure::DoNothing),
            None => Ok(OnClosure::DoNothing),
            _ => Err(zerror!(r#"Optional property `{PROP_STORAGE_ON_CLOSURE}` of S3 storage configurations must be either "do_nothing" (default) or "destroy_bucket""#).into())
        }
    }

    pub fn create_bucket_is_enabled(&self) -> bool {
        match self.config.volume_cfg.get(PROP_STORAGE_CREATE_BUCKET) {
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
