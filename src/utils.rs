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
use core::fmt;
use std::convert::TryFrom;
use zenoh::prelude::KeyExpr;
use zenoh::Result as ZResult;
use zenoh_keyexpr::OwnedKeyExpr;

pub struct S3Key<'a> {
    pub prefix: &'a Option<String>,
    pub key: String,
}

impl<'a> S3Key<'a> {
    pub fn from_key(prefix: &'a Option<String>, key: String) -> Self {
        Self { prefix, key }
    }

    pub fn from_key_expr(prefix: &'a Option<String>, key_expr: OwnedKeyExpr) -> ZResult<Self> {
        let mut key = key_expr.as_str();
        key = key.trim_start_matches('/');
        Ok(Self {
            prefix,
            key: key.to_string(),
        })
    }
}

impl From<S3Key<'_>> for String {
    fn from(s3_key: S3Key) -> Self {
        s3_key.prefix.as_ref().map_or_else(
            // For compatibility purposes between Amazon S3 and MinIO S3 implementations we trim
            // the '/' character.
            || s3_key.key.trim_start_matches('/').to_owned(),
            |prefix| s3_key.key.trim_start_matches(prefix).to_owned(),
        )
    }
}

impl TryFrom<&S3Key<'_>> for KeyExpr<'_> {
    type Error = zenoh_core::Error;
    fn try_from(s3_key: &S3Key) -> ZResult<Self> {
        s3_key.prefix.as_ref().map_or_else(
            || KeyExpr::try_from(s3_key.key.to_owned()),
            |prefix| {
                // For compatibility purposes between Amazon S3 and MinIO S3 implementations we
                // trim the '/' character.
                KeyExpr::try_from(format!("{}/{}", prefix, s3_key.key.trim_start_matches('/')))
            },
        )
    }
}

impl std::fmt::Display for S3Key<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.prefix {
            Some(prefix) => write!(f, "{}/{}", prefix, self.key),
            None => write!(f, "{}", self.key),
        }
    }
}
