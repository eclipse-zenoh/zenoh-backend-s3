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
use zenoh::value::Value;
use zenoh::Result as ZResult;
use zenoh_core::zerror;

pub struct S3Value {
    pub key: S3Key,
    pub value: Value,
}

pub struct S3Key {
    pub prefix: Option<String>,
    pub key: String,
}

impl S3Key {
    pub fn from_key(prefix: Option<String>, key: String) -> Self {
        Self { prefix, key }
    }

    pub fn from_key_expr(
        prefix: Option<String>,
        key_expr: zenoh::key_expr::KeyExpr<'_>,
    ) -> ZResult<Self> {
        let mut key = key_expr.as_str();
        if let Some(prefix) = prefix.to_owned() {
            key = key.strip_prefix(prefix.as_str()).ok_or_else(|| {
                zerror!(
                    "Received a Sample not starting with path_prefix '{}'",
                    prefix
                )
            })?;
        }
        key = key.trim_start_matches('/');
        Ok(Self {
            prefix,
            key: key.to_string(),
        })
    }
}

impl From<S3Key> for String {
    fn from(s3_key: S3Key) -> Self {
        s3_key.prefix.as_ref().map_or_else(
            || format!("/{}", s3_key.key.trim_start_matches('/')),
            |prefix| format!("{}/{}", prefix, s3_key.key.trim_start_matches('/')),
        )
    }
}

impl TryFrom<S3Key> for KeyExpr<'_> {
    type Error = zenoh_core::Error;
    fn try_from(s3_key: S3Key) -> ZResult<Self> {
        s3_key.prefix.as_ref().map_or_else(
            || KeyExpr::try_from(s3_key.key.to_owned()),
            |prefix| {
                KeyExpr::try_from(format!("{}/{}", prefix, s3_key.key.trim_start_matches('/')))
            },
        )
    }
}

impl std::fmt::Display for S3Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.prefix {
            Some(prefix) => write!(f, "{}/{}", prefix, self.key),
            None => write!(f, "{}", self.key),
        }
    }
}
