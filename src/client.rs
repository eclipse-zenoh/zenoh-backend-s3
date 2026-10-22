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

use std::{collections::HashMap, fmt};

use aws_config::Region;
use aws_sdk_s3::{
    config::Credentials,
    operation::{
        create_bucket::CreateBucketOutput, delete_object::DeleteObjectOutput,
        delete_objects::DeleteObjectsOutput, get_object::GetObjectOutput,
        head_bucket::HeadBucketError, head_object::HeadObjectOutput, put_object::PutObjectOutput,
    },
    primitives::ByteStream,
    types::{
        BucketLocationConstraint, CreateBucketConfiguration, Delete, Object, ObjectIdentifier,
    },
    Client,
};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use zenoh::{
    bytes::{Encoding, ZBytes},
    internal::zerror,
    Result as ZResult,
};

use crate::config::TlsClientConfig;

/// Client to communicate with the S3 storage.
pub(crate) struct S3Client {
    client: Client,
    bucket: String,
    region: Option<String>,
}

impl S3Client {
    /// Creates a new instance of the [S3Client].
    ///
    /// # Arguments
    ///
    /// * `credentials`: credentials to communicate with the storage
    /// * `bucket`: name of the bucket/storage
    /// * `region`: region where the bucket/storage ought to be located
    /// * `endpoint`: the endpoint where the storage is located, either an AWS endpoint
    ///   (see https://docs.aws.amazon.com/general/latest/gr/s3.html) or a custom one if you are
    ///   setting a MinIO instance. If None then the default AWS endpoint resolver will attempt
    ///   to retrieve the endpoint based on the specified region.
    /// * `tls_config`: optional TlsClientConfig to enable TLS security.
    pub async fn new(
        credentials: Option<Credentials>,
        bucket: String,
        region: Option<String>,
        endpoint: Option<String>,
        tls_config: Option<TlsClientConfig>,
    ) -> Self {
        let mut config_loader = aws_config::ConfigLoader::default();

        if let Some(creds) = credentials {
            config_loader = config_loader.credentials_provider(creds);
        }

        config_loader = match region {
            Some(ref region) => config_loader.region(Region::new(region.to_owned())),
            None => {
                // A region MUST be specified when using the aws_sdk_s3 crate independently of the fact
                // that it may not be used in the future (for instance when using MinIO).
                tracing::debug!("Region not specified. Setting 'us-east-1' region by default...");
                config_loader.region(Region::new("us-east-1"))
            }
        };

        if let Some(endpoint) = endpoint {
            config_loader = config_loader.endpoint_url(endpoint)
        }

        let sdk_config = &config_loader.load().await;
        let mut config = aws_sdk_s3::config::Builder::from(sdk_config).force_path_style(true);

        if let Some(tls_config) = tls_config {
            config = config.http_client(HyperClientBuilder::new().build(tls_config.https_connector))
        }

        let client = Client::from_conf(config.build());

        S3Client {
            client,
            bucket,
            region,
        }
    }

    /// Retrieves the object associated to the [key] specified.
    ///
    /// Returns `Ok(None)` if no object exists at [key].
    pub async fn get_object(&self, key: &str) -> ZResult<Option<GetObjectOutput>> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key.to_string())
            .send()
            .await;

        match result {
            Ok(output) => Ok(Some(output)),
            Err(err) => match err.into_service_error() {
                aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_) => Ok(None),
                err => Err(zerror!("Get operation failed for key '{key}': {err:?}").into()),
            },
        }
    }

    /// Retrieves the head object (the header of the object without its actual payload) associated
    /// to the [key] specified.
    pub async fn get_head_object(&self, key: &str) -> ZResult<HeadObjectOutput> {
        Ok(self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key.to_string())
            .send()
            .await?)
    }

    /// Performs a put operation on the storage on the key specified (which corresponds to the
    /// name of the file to be created) with the [Sample] provided.
    pub async fn put_object(
        &self,
        key: String,
        payload: ZBytes,
        encoding: Encoding,
        metadata: Option<HashMap<String, String>>,
    ) -> ZResult<PutObjectOutput> {
        let body = ByteStream::from(payload.to_bytes().to_vec());
        Ok(self
            .client
            .put_object()
            .bucket(self.bucket.to_owned())
            .key(key)
            .body(body)
            .set_content_encoding(Some(encoding.to_string()))
            .set_metadata(metadata)
            .send()
            .await?)
    }

    /// Performs a DELETE operation on the key specified.
    pub async fn delete_object(&self, key: String) -> ZResult<DeleteObjectOutput> {
        Ok(self
            .client
            .delete_object()
            .bucket(self.bucket.to_owned())
            .key(key)
            .send()
            .await?)
    }

    /// Deletes the specified objects from the bucket.
    pub async fn delete_objects_in_bucket(
        &self,
        objects: Vec<Object>,
    ) -> ZResult<DeleteObjectsOutput> {
        if objects.is_empty() {
            return Ok(DeleteObjectsOutput::builder()
                .set_deleted(Some(vec![]))
                .build());
        }

        let mut object_identifiers: Vec<ObjectIdentifier> = vec![];

        for object in objects {
            let identifier = ObjectIdentifier::builder()
                .set_key(object.key().map(|x| x.to_string()))
                .build()?;
            object_identifiers.push(identifier);
        }

        let delete = Delete::builder()
            .set_objects(Some(object_identifiers))
            .build()?;

        Ok(self
            .client
            .delete_objects()
            .bucket(self.bucket.to_owned())
            .delete(delete)
            .send()
            .await?)
    }

    /// Ensures the bucket associated to this client is available.
    ///
    /// The bucket existence is first checked with a read-only `HeadBucket` call (which only
    /// requires the `s3:ListBucket` permission) so that the mutating `CreateBucket` operation is
    /// only issued when the bucket is actually missing.
    ///
    /// Returns:
    /// - `Ok(Some(CreateBucketOutput))` when the bucket was created
    /// - `Ok(None)` when the bucket already exists and `reuse_bucket` is true
    /// - `Err` in any other case (bucket already exists while `reuse_bucket` is false, missing
    ///   permissions, network error, ...)
    pub async fn create_bucket(&self, reuse_bucket: bool) -> ZResult<Option<CreateBucketOutput>> {
        // Read-only existence check (only requires the s3:ListBucket permission).
        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => {
                // The bucket exists and is accessible.
                if reuse_bucket {
                    Ok(None)
                } else {
                    Err(zerror!(
                        "Bucket '{self}' already exists and is accessible while 'reuse_bucket' \
                         is set to false in the configuration."
                    )
                    .into())
                }
            }
            Err(err) => match err.into_service_error() {
                HeadBucketError::NotFound(_) => {
                    if reuse_bucket {
                        tracing::info!(
                            "Bucket '{self}' not found despite 'reuse_bucket' being enabled; creating it..."
                        );
                    }
                    self.do_create_bucket().await.map(Some)
                }
                err => {
                    Err(zerror!("Couldn't check the existence of bucket '{self}': {err:?}.").into())
                }
            },
        }
    }

    /// Creates the bucket associated to this client with the default configuration.
    async fn do_create_bucket(&self) -> ZResult<CreateBucketOutput> {
        let constraint = self
            .region
            .as_ref()
            .map(|region| BucketLocationConstraint::from(region.as_str()));
        let cfg = CreateBucketConfiguration::builder()
            .set_location_constraint(constraint)
            .build();
        self.client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(self.bucket.to_owned())
            .send()
            .await
            .map_err(|err| zerror!("Couldn't create bucket '{self}': {err:?}.").into())
    }

    /// Deletes the bucket associated to this storage.
    ///
    /// In order to fulfill this operation, all the contained files in the bucket are deleted.
    pub async fn delete_bucket(&self) -> ZResult<()> {
        let objects = self.list_objects_in_bucket().await?;
        self.delete_objects_in_bucket(objects).await?;
        self.client
            .delete_bucket()
            .bucket(&self.bucket)
            .send()
            .await?;
        tracing::debug!("Deleted bucket '{}'.", self.bucket.to_owned());
        Ok(())
    }

    /// Lists all the objects contained in the bucket.
    pub async fn list_objects_in_bucket(&self) -> ZResult<Vec<Object>> {
        let response = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.to_owned())
            .send()
            .await?;
        Ok(response.contents().to_vec())
    }
}

impl std::fmt::Display for S3Client {
    // It's sufficient to display the bucket name as we only have a single
    // bucket and a single storage for each client.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.bucket)
    }
}
