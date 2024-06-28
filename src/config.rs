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

use std::{fs::File, io::BufReader};

use async_rustls::rustls::{ClientConfig, OwnedTrustAnchor, RootCertStore};
use aws_sdk_s3::config::Credentials;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use rustls_pki_types::CertificateDer;
use serde_json::{Map, Value};
use webpki::TrustAnchor;
use zenoh::{internal::zerror, key_expr::OwnedKeyExpr, Result as ZResult};
use zenoh_backend_traits::config::{PrivacyGetResult, PrivacyTransparentGet, StorageConfig};

// Properties used by the Backend
const PROP_S3_ACCESS_KEY: &str = "access_key";
const PROP_S3_BUCKET: &str = "bucket";
const PROP_S3_SECRET_KEY: &str = "secret_key";

// Properties used by the Storage
const PROP_STORAGE_REUSE_BUCKET: &str = "reuse_bucket";
const PROP_STORAGE_READ_ONLY: &str = "read_only";
const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

const DEFAULT_PROVIDER: &str = "zenoh-s3-backend";

// TLS properties
pub const TLS_PROP: &str = "tls";
pub const TLS_ROOT_CA_CERTIFICATE_FILE: &str = "root_ca_certificate_file";
pub const TLS_ROOT_CA_CERTIFICATE_BASE64: &str = "root_ca_certificate_base64";

pub enum OnClosure {
    DestroyBucket,
    DoNothing,
}

/// Struct to contain all the information necessary for the proper communication with the s3
/// storage. This information is loaded from a [StorageConfig] instance which contains the
/// values from the `storages` field on the `.json5` storage configuration file which looks like
/// follows:
///
/// ```
/// storages: {
///    s3_storage: {
///      key_expr: "s3/example/*",
///      strip_prefix: "s3/example",
///      volume: {
///        id: "s3",
///        reuse_bucket: true,
///        bucket: "zenoh-test-bucket",
///        on_closure: "destroy_bucket",
///        private: {
///            access_key: "AKIAIOSFODNN7EXAMPLE",
///            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
///        }
///      }
///    },
///  }
/// ```
///
/// The fields of the struct have the following purposes:
///
/// * credentials: is loaded from the access_key_id and secret_key_id set in the config file which
///     were previously set in the S3 configuration in order to grant permissions to a user to
///     perform operations such as read, write, create bucket, delete bucket...
/// * bucket: name of the bucket the storage is associated to
/// * path_prefix: the path prefix stated under the `strip_prefix` value of the configuration file.
///     This prefix needs to match the key expression associated to this storage (otherwise Error
///     is returned) as it will be used to strip the prefix of the incoming queries. For instance
///     if we receive a PUT operation on the s3/example/test and the `strip_prefix` value was
///     s3/example, then the storage will try to perform a PUT operation with /test.
/// * key_expr: the provided key expression.
/// * is_read_only: if the storage is configured to be read only
/// * on_closure: the operation to be performed on the storage upon destruction, either
///     `destroy_bucket` or `do_nothing`. When setting `destroy_bucket` then the config field
///     `adminspace.permissions.write` must be set to true for the operation to succeed.
/// * admin_status: the json value of the [StorageConfig]
/// * reuse_bucket_is_enabled: the storage attempts to create the bucket but if the bucket
///     was already created and is owned by you then the storage is associated to that preexisting
///     bucket.
pub(crate) struct S3Config {
    pub credentials: Credentials,
    pub bucket: String,
    pub path_prefix: Option<String>,
    pub key_expr: OwnedKeyExpr,
    pub is_read_only: bool,
    pub on_closure: OnClosure,
    pub admin_status: serde_json::Value,
    pub reuse_bucket_is_enabled: bool,
}

impl S3Config {
    /// Creates a new instance of [S3Config] from the StorageConfig passed as a parameter.
    pub async fn new(config: &StorageConfig) -> ZResult<Self> {
        let credentials = S3Config::load_credentials(config)?;
        let path_prefix = S3Config::load_path_prefix(config)?;
        let key_expr = config.key_expr.to_owned();
        let bucket = S3Config::load_bucket_name(config)?;
        let is_read_only = S3Config::is_read_only(config)?;
        let on_closure = S3Config::load_on_closure(config)?;
        let reuse_bucket_is_enabled = S3Config::reuse_bucket_is_enabled(config);
        let admin_status = config.to_json_value();
        Ok(S3Config {
            credentials,
            bucket,
            path_prefix,
            key_expr,
            is_read_only,
            on_closure,
            admin_status,
            reuse_bucket_is_enabled,
        })
    }

    fn load_credentials(config: &StorageConfig) -> ZResult<Credentials> {
        let volume_cfg = config.volume_cfg.as_object().ok_or_else(|| {
            zerror!("Couldn't retrieve private properties of the storage from json5 config file.")
        })?;

        let access_key = get_private_conf(volume_cfg, PROP_S3_ACCESS_KEY)
            .map_err(|err| zerror!("Could not load '{}': {}", PROP_S3_ACCESS_KEY, err))?
            .ok_or_else(|| zerror!("Property '{PROP_S3_ACCESS_KEY}' needs to be of specified!"))?;

        let secret_key = get_private_conf(volume_cfg, PROP_S3_SECRET_KEY)?
            .ok_or_else(|| zerror!("Property '{PROP_S3_SECRET_KEY}' needs to be of specified!"))?;

        Ok(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            DEFAULT_PROVIDER,
        ))
    }

    fn load_bucket_name(config: &StorageConfig) -> ZResult<String> {
        Ok(match config.volume_cfg.get(PROP_S3_BUCKET) {
            Some(serde_json::Value::String(name)) => Ok(name.to_owned()),
            _ => Err(zerror!("Property '{PROP_S3_BUCKET}' was not specified!")),
        }?)
    }

    fn load_path_prefix(config: &StorageConfig) -> ZResult<Option<String>> {
        config.strip_prefix.to_owned().map_or_else(
            || Ok(None),
            |prefix| {
                let prefix = prefix.to_string();
                let path_expr = config.key_expr.to_owned();
                if !path_expr.starts_with(&prefix) {
                    Err(zerror!(
                        r#"The specified "strip_prefix={}" is not a prefix of "key_expr={}""#,
                        prefix,
                        path_expr
                    )
                    .into())
                } else {
                    Ok(Some(prefix))
                }
            },
        )
    }

    fn is_read_only(config: &StorageConfig) -> ZResult<bool> {
        match config.volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => Ok(false),
            Some(serde_json::Value::Bool(true)) => Ok(true),
            _ => Err(zerror!(
                "Optional property `{PROP_STORAGE_READ_ONLY}` of s3 storage 
                    configurations must be a boolean"
            )
            .into()),
        }
    }

    fn load_on_closure(config: &StorageConfig) -> ZResult<OnClosure> {
        match config.volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_bucket" => {
                Ok(OnClosure::DestroyBucket)
            }
            Some(serde_json::Value::String(s)) if s == "do_nothing" => Ok(OnClosure::DoNothing),
            None => Ok(OnClosure::DoNothing),
            _ => Err(zerror!(
                r#"Optional property `{PROP_STORAGE_ON_CLOSURE}` of S3 storage
            configurations must be either "do_nothing" (default) or "destroy_bucket""#
            )
            .into()),
        }
    }

    fn reuse_bucket_is_enabled(config: &StorageConfig) -> bool {
        match config.volume_cfg.get(PROP_STORAGE_REUSE_BUCKET) {
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
            tracing::warn!(
                r#"Value "{}" is given for `{}` publicly (i.e. is visible by anyone who can fetch
                the router configuration). You may want to replace `{}: "{}"` with `private: 
                {{{}: "{}"}}`"#,
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
            tracing::warn!(
                r#"Value "{}" is given for `{}` publicly, but a private value also exists. 
                The private value will be used, but the public value, which is {} the same as
                 the private one, will still be visible in configurations."#,
                public,
                credit,
                if public == private { "" } else { "not " }
            );
            Ok(Some(private))
        }
        _ => Err(zerror!("Optional property `{}` must be a string", credit))?,
    }
}

/// Utility to load the tls related configuration and establish proper communication with a MinIO
/// server with TLS enabled.
#[derive(Clone)]
pub(crate) struct TlsClientConfig {
    pub https_connector: HttpsConnector<HttpConnector>,
}

impl TlsClientConfig {
    /// Creates a new instance of [TlsClientConfig] from the configuration specified in the config
    /// file.
    pub fn new(tls_config: &Map<String, Value>) -> ZResult<Self> {
        tracing::debug!("Loading TLS config values...");

        // Allows mixed user-generated CA and webPKI CA
        tracing::debug!("Loading default Web PKI certificates.");
        let mut root_cert_store: RootCertStore = RootCertStore {
            roots: Self::load_default_webpki_certs().roots,
        };

        if let Some(root_ca_cert_file) = get_private_conf(tls_config, TLS_ROOT_CA_CERTIFICATE_FILE)?
        {
            tracing::debug!("Loading certificate specified under {TLS_ROOT_CA_CERTIFICATE_FILE}.");
            Self::load_root_ca_certificate_file_trust_anchors(
                root_ca_cert_file,
                &mut root_cert_store,
            )?;
        } else if let Some(root_ca_cert_base64) =
            get_private_conf(tls_config, TLS_ROOT_CA_CERTIFICATE_BASE64)?
        {
            tracing::debug!(
                "Loading certificate specified under {TLS_ROOT_CA_CERTIFICATE_BASE64}."
            );
            Self::load_root_ca_certificate_base64_trust_anchors(
                root_ca_cert_base64,
                &mut root_cert_store,
            )?;
        }

        let client_config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let rustls_connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(client_config)
            .https_only()
            .enable_http1()
            .build();
        Ok(TlsClientConfig {
            https_connector: rustls_connector,
        })
    }

    fn load_root_ca_certificate_file_trust_anchors(
        root_ca_cert_file: &String,
        root_cert_store: &mut RootCertStore,
    ) -> ZResult<()> {
        if root_ca_cert_file.is_empty() {
            tracing::warn!(
                "Provided an empty value for `{TLS_ROOT_CA_CERTIFICATE_FILE}`. Ignornig..."
            );
            return Ok(());
        };

        let mut pem = BufReader::new(File::open(root_ca_cert_file)?);
        let certs: Vec<CertificateDer> =
            rustls_pemfile::certs(&mut pem).collect::<Result<_, _>>()?;
        let trust_anchors = certs.iter().map(|cert| {
            let ta = TrustAnchor::try_from_cert_der(&cert[..]).unwrap();
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        });
        root_cert_store.add_trust_anchors(trust_anchors.into_iter());
        Ok(())
    }

    fn load_root_ca_certificate_base64_trust_anchors(
        b64_certificate: &str,
        root_cert_store: &mut RootCertStore,
    ) -> ZResult<()> {
        if b64_certificate.is_empty() {
            tracing::warn!(
                "Provided an empty value for `{TLS_ROOT_CA_CERTIFICATE_BASE64}`. Ignornig..."
            );
            return Ok(());
        };
        let certificate_pem = Self::base64_decode(b64_certificate)?;
        let mut pem = BufReader::new(certificate_pem.as_slice());
        let certs: Vec<CertificateDer> =
            rustls_pemfile::certs(&mut pem).collect::<Result<_, _>>()?;
        let trust_anchors = certs.iter().map(|cert| {
            let ta = TrustAnchor::try_from_cert_der(&cert[..]).unwrap();
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        });
        root_cert_store.add_trust_anchors(trust_anchors.into_iter());
        Ok(())
    }

    fn load_default_webpki_certs() -> RootCertStore {
        let mut root_cert_store = RootCertStore::empty();
        root_cert_store.add_trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().map(|ta| {
            OwnedTrustAnchor::from_subject_spki_name_constraints(
                ta.subject,
                ta.spki,
                ta.name_constraints,
            )
        }));
        root_cert_store
    }

    pub fn base64_decode(data: &str) -> ZResult<Vec<u8>> {
        use base64::{engine::general_purpose, Engine};
        Ok(general_purpose::STANDARD
            .decode(data)
            .map_err(|e| zerror!("Unable to perform base64 decoding: {e:?}"))?)
    }
}
