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

use zenoh::{key_expr::OwnedKeyExpr, Result as ZResult};

pub struct S3Key<'a> {
    pub prefix: Option<&'a String>,
    pub key_expr: OwnedKeyExpr,
}

impl<'a> S3Key<'a> {
    pub fn from_key(prefix: Option<&'a String>, key: String) -> ZResult<Self> {
        let key_expr = match prefix {
            Some(prefix) => {
                OwnedKeyExpr::try_from(format!("{}/{}", prefix, key.trim_start_matches('/')))?
            }
            None => OwnedKeyExpr::try_from(key.trim_start_matches('/'))?,
        };
        Ok(Self { prefix, key_expr })
    }

    pub fn from_key_expr(prefix: Option<&'a String>, key_expr: OwnedKeyExpr) -> ZResult<Self> {
        let key_expr = match prefix {
            Some(prefix) => {
                OwnedKeyExpr::try_from(format!("{}/{}", prefix, key_expr.trim_start_matches('/')))?
            }
            None => key_expr,
        };
        Ok(Self { prefix, key_expr })
    }
}

impl From<S3Key<'_>> for String {
    fn from(s3_key: S3Key) -> Self {
        s3_key.prefix.map_or_else(
            // For compatibility purposes between Amazon S3 and MinIO S3 implementations we trim
            // the '/' character.
            || s3_key.key_expr.trim_start_matches('/').to_owned(),
            |prefix| s3_key.key_expr.trim_start_matches(prefix).to_owned(),
        )
    }
}

impl std::fmt::Display for S3Key<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.prefix {
            Some(prefix) => write!(f, "{}/{}", prefix, self.key_expr),
            None => write!(f, "{}", self.key_expr),
        }
    }
}
