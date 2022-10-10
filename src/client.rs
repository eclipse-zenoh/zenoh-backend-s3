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

use std::fmt;

use aws_sdk_s3::model::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, Object, ObjectIdentifier,
};
use aws_sdk_s3::output::{
    CreateBucketOutput, DeleteObjectOutput, DeleteObjectsOutput, GetObjectOutput,
};
use aws_sdk_s3::{output::PutObjectOutput, types::ByteStream, Client};
use aws_sdk_s3::{Credentials, Endpoint, Region};
use zenoh::sample::Sample;
use zenoh::Result as ZResult;
use zenoh_buffers::SplitBuffer;

pub struct S3Client {
    client: Client,
    bucket: String,
    region: Region,
}

impl S3Client {
    pub async fn new(
        credentials: Credentials,
        region: Region,
        bucket: String,
        endpoint: String,
    ) -> Self {
        let sdk_config = aws_config::ConfigLoader::default()
            .endpoint_resolver(Endpoint::immutable(
                endpoint.parse().expect("Invalid endpoint: "),
            ))
            .region(region.to_owned())
            .credentials_provider(credentials)
            .load()
            .await;

        let client = Client::new(&sdk_config);
        S3Client {
            client,
            bucket: bucket.to_string(),
            region,
        }
    }

    pub async fn get_object(&self, key: &str) -> ZResult<GetObjectOutput> {
        Ok(self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key.to_string())
            .send()
            .await?)
    }

    pub async fn put_object(&self, key: String, sample: Sample) -> ZResult<PutObjectOutput> {
        let body = ByteStream::from(sample.payload.contiguous().to_vec());
        Ok(self
            .client
            .put_object()
            .bucket(self.bucket.to_owned())
            .key(key)
            .body(body)
            .set_content_encoding(Some(sample.encoding.to_string()))
            .send()
            .await?)
    }

    pub async fn delete_object(&self, key: String) -> ZResult<DeleteObjectOutput> {
        Ok(self
            .client
            .delete_object()
            .bucket(self.bucket.to_owned())
            .key(key)
            .send()
            .await?)
    }

    pub async fn list_objects_in_bucket(&self) -> ZResult<Vec<Object>> {
        let response = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.to_owned())
            .send()
            .await?;
        Ok(response.contents().unwrap_or_default().to_vec())
    }

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
                .build();
            object_identifiers.push(identifier);
        }

        let delete = Delete::builder()
            .set_objects(Some(object_identifiers))
            .build();

        Ok(self
            .client
            .delete_objects()
            .bucket(self.bucket.to_owned())
            .delete(delete)
            .send()
            .await?)
    }

    /// Asyncronically creates the bucket associated to this client upon construction.
    #[tokio::main]
    pub async fn create_bucket(&self) -> ZResult<CreateBucketOutput> {
        let constraint = BucketLocationConstraint::from(self.region.to_string().as_str());
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        Ok(self
            .client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(self.bucket.to_owned())
            .send()
            .await?)
    }

    pub async fn delete_bucket(&self) -> ZResult<()> {
        let objects = self.list_objects_in_bucket().await?;
        self.delete_objects_in_bucket(objects).await?;
        self.client
            .delete_bucket()
            .bucket(&self.bucket)
            .send()
            .await?;
        log::debug!("Deleted bucket '{}'.", self.bucket.to_owned());
        Ok(())
    }
}

impl fmt::Debug for S3Client {
    // It's sufficient to display the bucket name as we only have a single
    // bucket and a single storage for each client.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("").field(&self.bucket).finish()
    }
}
