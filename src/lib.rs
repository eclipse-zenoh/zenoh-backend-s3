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

use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use log::{debug, error, trace, warn};
use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::convert::TryInto;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uhlc::NTP64;
use zenoh::buf::{WBuf, ZBuf};
use zenoh::prelude::*;
use zenoh::time::{new_reception_timestamp, Timestamp};
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{StorageConfig, VolumeConfig};
use zenoh_backend_traits::StorageInsertionResult;
use zenoh_backend_traits::*;
use zenoh_buffers::traits::{reader::HasReader, SplitBuffer};
use zenoh_collections::{Timed, TimedEvent, Timer};
use zenoh_core::{bail, zerror};
use zenoh_protocol::io::ZBufCodec;
use zenoh_util::zenoh_home;

/// The environement variable used to configure the root of all storages managed by this RocksdbBackend.
pub const SCOPE_ENV_VAR: &str = "ZBACKEND_ROCKSDB_ROOT";

/// The default root (whithin zenoh's home directory) if the ZBACKEND_ROCKSDB_ROOT environment variable is not specified.
pub const DEFAULT_ROOT_DIR: &str = "zbackend_rocksdb";

// Properies used by the Backend
//  - None

// Properies used by the Storage
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_CREATE_DB: &str = "create_db";
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

// Column family names
const CF_PAYLOADS: &str = rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
const CF_DATA_INFO: &str = "data_info";

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static! {
    static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
    static ref GC_PERIOD: Duration = Duration::new(5, 0);
    static ref MIN_DELAY_BEFORE_REMOVAL: NTP64 = NTP64::from(Duration::new(5, 0));
}

pub(crate) enum OnClosure {
    DestroyDB,
    DoNothing,
}

#[allow(dead_code)]
const CREATE_BACKEND_TYPECHECK: CreateVolume = create_volume;

#[no_mangle]
pub fn create_volume(_unused: VolumeConfig) -> ZResult<Box<dyn Volume>> {
    // For some reasons env_logger is sometime not active in a loaded library.
    // Try to activate it here, ignoring failures.
    let _ = env_logger::try_init();
    debug!("RocksDB backend {}", LONG_VERSION.as_str());

    let root = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
        PathBuf::from(dir)
    } else {
        let mut dir = PathBuf::from(zenoh_home());
        dir.push(DEFAULT_ROOT_DIR);
        dir
    };
    let mut properties = Properties::default();
    properties.insert("root".into(), root.to_string_lossy().into());
    properties.insert("version".into(), LONG_VERSION.clone());

    let admin_status = properties
        .0
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();
    Ok(Box::new(RocksdbBackend { admin_status, root }))
}

pub struct RocksdbBackend {
    admin_status: serde_json::Value,
    root: PathBuf,
}

#[async_trait]
impl Volume for RocksdbBackend {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    async fn create_storage(&mut self, config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let path_expr = config.key_expr.clone();
        let path_prefix = config.strip_prefix.clone();
        if !path_expr.starts_with(&path_prefix) {
            bail!(
                r#"The specified "strip_prefix={}" is not a prefix of "key_expr={}""#,
                path_prefix,
                path_expr
            )
        }

        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("rocksdb backed storages need volume-specific configurations"),
        };

        let read_only = match volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => false,
            Some(serde_json::Value::Bool(true)) => true,
            _ => {
                bail!(
                    "Optional property `{}` of rocksdb storage configurations must be a boolean",
                    PROP_STORAGE_READ_ONLY
                )
            }
        };

        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_db" => OnClosure::DestroyDB,
            Some(serde_json::Value::String(s)) if s == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            _ => {
                bail!(
                    r#"Optional property `{}` of rocksdb storage configurations must be either "do_nothing" (default) or "destroy_db""#,
                    PROP_STORAGE_ON_CLOSURE
                )
            }
        };

        let db_path = match volume_cfg.get(PROP_STORAGE_DIR) {
            Some(serde_json::Value::String(dir)) => {
                let mut db_path = self.root.clone();
                db_path.push(dir);
                db_path
            }
            _ => {
                bail!(
                    r#"Required property `{}` for File System Storage must be a string"#,
                    PROP_STORAGE_DIR
                )
            }
        };

        let mut opts = Options::default();
        match volume_cfg.get(PROP_STORAGE_CREATE_DB) {
            Some(serde_json::Value::Bool(true)) => opts.create_if_missing(true),
            Some(serde_json::Value::Bool(false)) | None => {}
            _ => {
                bail!(
                    r#"Optional property `{}` of rocksdb storage configurations must be a boolean"#,
                    PROP_STORAGE_CREATE_DB
                )
            }
        }
        opts.create_missing_column_families(true);
        let db = if read_only {
            DB::open_cf_for_read_only(&opts, &db_path, &[CF_PAYLOADS, CF_DATA_INFO], true)
        } else {
            let cf_payloads = ColumnFamilyDescriptor::new(CF_PAYLOADS, opts.clone());
            let cf_data_info = ColumnFamilyDescriptor::new(CF_DATA_INFO, opts.clone());
            DB::open_cf_descriptors(&opts, &db_path, vec![cf_payloads, cf_data_info])
        }
        .map_err(|e| {
            zerror!(
                "Failed to open data-info database from {:?}: {}",
                db_path,
                e
            )
        })?;
        let db = Arc::new(Mutex::new(Some(db)));

        let timer = if read_only {
            None
        } else {
            // start periodic GC event
            let t = Timer::default();
            let gc = TimedEvent::periodic(*GC_PERIOD, GarbageCollectionEvent { db: db.clone() });
            let _ = t.add_async(gc).await;
            Some(t)
        };
        Ok(Box::new(RocksdbStorage {
            admin_status: config,
            path_prefix,
            on_closure,
            read_only,
            db,
            timer,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }
}

struct RocksdbStorage {
    admin_status: StorageConfig,
    path_prefix: String,
    on_closure: OnClosure,
    read_only: bool,
    // Note: rocksdb isn't thread-safe. See https://github.com/rust-rocksdb/rust-rocksdb/issues/404
    db: Arc<Mutex<Option<DB>>>,
    // Note: Timer is kept to not be dropped and keep the GC periodic event running
    #[allow(dead_code)]
    timer: Option<Timer>,
}

#[async_trait]
impl Storage for RocksdbStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.to_json_value()
    }

    // When receiving a Sample (i.e. on PUT or DELETE operations)
    async fn on_sample(&mut self, sample: Sample) -> ZResult<StorageInsertionResult> {
        // the key in rocksdb is the path stripped from "path_prefix"
        let key = sample
            .key_expr
            .try_as_str()?
            .strip_prefix(&self.path_prefix)
            .ok_or_else(|| {
                zerror!(
                    "Received a Sample not starting with path_prefix '{}'",
                    self.path_prefix
                )
            })?;

        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // get latest timestamp for this key (if already exists in db)
        // and drop incoming sample if older
        let sample_ts = sample.timestamp.unwrap_or_else(new_reception_timestamp);
        if let Some(old_ts) = get_timestamp(db, key)? {
            if sample_ts < old_ts {
                debug!(
                    "{} on {} dropped: out-of-date",
                    sample.kind, sample.key_expr
                );
                return Ok(StorageInsertionResult::Outdated);
            }
        }

        // Store or delete the sample depending the ChangeKind
        match sample.kind {
            SampleKind::Put => {
                if !self.read_only {
                    // put payload and data_info in DB
                    put_kv(db, key, sample.value, sample_ts)
                } else {
                    warn!("Received PUT for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Delete => {
                if !self.read_only {
                    // delete file
                    delete_kv(db, key, sample_ts)
                } else {
                    warn!("Received DELETE for read-only DB on {:?} - ignored", key);
                    Err("Received update for read-only DB".into())
                }
            }
            SampleKind::Patch => {
                warn!("Received PATCH for {}: not yet supported", sample.key_expr);
                Ok(StorageInsertionResult::Outdated)
            }
        }
    }

    // When receiving a Query (i.e. on GET operations)
    async fn on_query(&mut self, query: Query) -> ZResult<()> {
        // get the query's Selector
        let selector = query.selector();

        // get the list of sub-path expressions that will match the same stored keys than
        // the selector, if those keys had the path_prefix.
        let sub_selectors =
            utils::get_sub_key_selectors(selector.key_selector.try_as_str()?, &self.path_prefix);
        debug!(
            "Query on {} with path_prefix={} => sub_selectors = {:?}",
            selector.key_selector, self.path_prefix, sub_selectors
        );

        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Get all matching keys/values
        let mut kvs: Vec<(String, Value, Timestamp)> = Vec::with_capacity(sub_selectors.len());
        for sub_selector in sub_selectors {
            if sub_selector.contains('*') {
                find_matching_kv(db, sub_selector, &mut kvs);
            } else {
                // path_expr correspond to 1 key. Get it.
                match get_kv(db, sub_selector) {
                    Ok(Some((value, timestamp))) => {
                        kvs.push((sub_selector.into(), value, timestamp))
                    }
                    Ok(None) => (), // key not found, do nothing
                    Err(e) => warn!(
                        "Replying to query on {} : failed get key {} : {}",
                        selector, sub_selector, e
                    ),
                }
            }
        }

        // Release lock on DB
        drop(db_cell);

        // Send replies
        for (key, value, timestamp) in kvs {
            // append path_prefix to the key
            let res_name = concat_str(&self.path_prefix, &key);
            query
                .reply(Sample::new(res_name, value).with_timestamp(timestamp))
                .await;
        }

        Ok(())
    }

    async fn get_all_entries(&self) -> ZResult<Vec<(String, Timestamp)>> {
        let mut result = Vec::new();

        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Iterate over DATA_INFO Column Family to avoid loading payloads
        for (key, buf) in
            db.prefix_iterator_cf(db.cf_handle(CF_DATA_INFO).unwrap(), &self.path_prefix)
        {
            let key_str = String::from_utf8_lossy(&key);
            if let Ok((_, timestamp, _)) = decode_data_info(&buf) {
                result.push((key_str.into_owned(), timestamp))
            } else {
                warn!(
                    "Getting all entries : failed to decode data_info for key {}",
                    key_str
                );
                return Err("Getting all entries : failed to decode data_info".into());
            }
        }

        Ok(result)
    }
}

impl Drop for RocksdbStorage {
    fn drop(&mut self) {
        async_std::task::block_on(async move {
            // Get lock on DB and take DB so we can drop it before destroying it
            // (avoiding RocksDB lock to be taken twice)
            let mut db_cell = self.db.lock().await;
            let db = db_cell.take().unwrap();

            // Stop GC
            if let Some(t) = &mut self.timer {
                t.stop_async().await;
            }

            // Flush all
            if let Err(err) = db.flush() {
                warn!("Closing Rocksdb storage, flush failed: {}", err);
            }

            // copy path for later use after DB is dropped
            let path = db.path().to_path_buf();

            // drop DB, releasing RocksDB lock
            drop(db);

            match self.on_closure {
                OnClosure::DestroyDB => {
                    debug!(
                        "Close Rocksdb storage, destroying database {}",
                        path.display()
                    );
                    if let Err(err) = DB::destroy(&Options::default(), &path) {
                        error!(
                            "Failed to destroy Rocksdb database '{}' : {}",
                            path.display(),
                            err
                        );
                    }
                }
                OnClosure::DoNothing => {
                    debug!(
                        "Close Rocksdb storage, keeping database {} as it is",
                        path.display()
                    );
                }
            }
        });
    }
}

fn put_kv(
    db: &DB,
    key: &str,
    value: Value,
    timestamp: Timestamp,
) -> ZResult<StorageInsertionResult> {
    trace!("Put key {} in {:?}", key, db);
    let data_info = encode_data_info(&value.encoding, &timestamp, false)?;

    // Write content and encoding+timestamp in different Column Families
    let mut batch = WriteBatch::default();
    batch.put_cf(
        db.cf_handle(CF_PAYLOADS).unwrap(),
        key,
        value.payload.contiguous(),
    );
    batch.put_cf(
        db.cf_handle(CF_DATA_INFO).unwrap(),
        key,
        data_info.get_first_slice(..),
    );
    match db.write(batch) {
        Ok(()) => Ok(StorageInsertionResult::Inserted),
        Err(e) => Err(rocksdb_err_to_zerr(e)),
    }
}

fn delete_kv(db: &DB, key: &str, timestamp: Timestamp) -> ZResult<StorageInsertionResult> {
    trace!("Delete key {} from {:?}", key, db);
    let data_info = encode_data_info(&Encoding::EMPTY, &timestamp, true)?;

    // Delete key from CF_PAYLOADS Column Family
    // Put deletion timestamp into CF_DATA_INFO Column Family (to avoid re-insertion of older value)
    let mut batch = WriteBatch::default();
    batch.delete_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key);
    batch.put_cf(
        db.cf_handle(CF_DATA_INFO).unwrap(),
        key,
        data_info.get_first_slice(..),
    );
    match db.write(batch) {
        Ok(()) => Ok(StorageInsertionResult::Deleted),
        Err(e) => Err(rocksdb_err_to_zerr(e)),
    }
}

fn get_kv(db: &DB, key: &str) -> ZResult<Option<(Value, Timestamp)>> {
    trace!("Get key {} from {:?}", key, db);
    // TODO: use MultiGet when available (see https://github.com/rust-rocksdb/rust-rocksdb/issues/485)
    match (
        db.get_cf(db.cf_handle(CF_PAYLOADS).unwrap(), key),
        db.get_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key),
    ) {
        (Ok(Some(payload)), Ok(Some(info))) => {
            let (encoding, timestamp, deleted) = decode_data_info(&info)?;
            if deleted {
                Ok(None)
            } else {
                Ok(Some((
                    Value {
                        payload: payload.into(),
                        encoding,
                    },
                    timestamp,
                )))
            }
        }
        (Ok(Some(payload)), Ok(None)) => {
            // Only the payload is present in DB!
            // Possibly legacy data. Consider as encoding as APP_OCTET_STREAM and create timestamp from now()
            Ok(Some((
                Value {
                    payload: payload.into(),
                    encoding: Encoding::APP_OCTET_STREAM,
                },
                new_reception_timestamp(),
            )))
        }
        (Ok(None), _) => Ok(None),
        (Err(err), _) | (_, Err(err)) => Err(rocksdb_err_to_zerr(err)),
    }
}

fn find_matching_kv(db: &DB, sub_selector: &str, results: &mut Vec<(String, Value, Timestamp)>) {
    // Use Rocksdb prefix seek for faster search
    let prefix = &sub_selector[..sub_selector.find('*').unwrap()];
    trace!(
        "Find keys matching {} from {:?} using prefix seek with '{}'",
        sub_selector,
        db,
        prefix
    );

    // Iterate over DATA_INFO Column Family to avoid loading payloads possibly for nothing if not matching
    for (key, buf) in db.prefix_iterator_cf(db.cf_handle(CF_DATA_INFO).unwrap(), prefix) {
        if let Ok(false) = decode_deleted_flag(&buf) {
            let key_str = String::from_utf8_lossy(&key);
            if zenoh::utils::key_expr::intersect(&key_str, sub_selector) {
                match db.get_cf(db.cf_handle(CF_PAYLOADS).unwrap(), &key) {
                    Ok(Some(payload)) => {
                        if let Ok((encoding, timestamp, _)) = decode_data_info(&buf) {
                            results.push((
                                key_str.into_owned(),
                                Value {
                                    payload: payload.into(),
                                    encoding,
                                },
                                timestamp,
                            ))
                        } else {
                            warn!(
                                "Replying to query on {} : failed to decode data_info for key {}",
                                sub_selector, key_str
                            )
                        }
                    }
                    Ok(None) => (), // data_info exists, but not payload: key was probably deleted
                    Err(err) => warn!(
                        "Replying to query on {} : failed get key {} : {}",
                        sub_selector, key_str, err
                    ),
                }
            }
        }
    }
}

fn get_timestamp(db: &DB, key: &str) -> ZResult<Option<Timestamp>> {
    match db.get_pinned_cf(db.cf_handle(CF_DATA_INFO).unwrap(), key) {
        Ok(Some(pin_val)) => decode_timestamp(pin_val.as_ref()).map(Some),
        Ok(None) => {
            trace!("timestamp for {:?} not found", key);
            Ok(None)
        }
        Err(e) => bail!("Failed to get data-info for {:?}: {}", key, e),
    }
}

fn encode_data_info(encoding: &Encoding, timestamp: &Timestamp, deleted: bool) -> ZResult<WBuf> {
    let mut result: WBuf = WBuf::new(32, true);
    // note: encode timestamp at first for faster decoding when only this one is required
    let write_ok = result.write_timestamp(timestamp)
        && result.write_zint(deleted as ZInt)
        && result.write_zint(u8::from(*encoding.prefix()).into())
        && result.write_string(&*encoding.suffix());
    if !write_ok {
        bail!("Failed to encode data-info")
    } else {
        Ok(result)
    }
}

fn decode_data_info(buf: &[u8]) -> ZResult<(Encoding, Timestamp, bool)> {
    let buf = ZBuf::from(buf.to_vec());
    let mut buf = buf.reader();
    let timestamp = buf
        .read_timestamp()
        .ok_or_else(|| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted = buf
        .read_zint()
        .ok_or_else(|| zerror!("Failed to decode data-info (encoding.prefix)"))?
        != 0;
    let encoding_prefix = buf
        .read_zint()
        .ok_or_else(|| zerror!("Failed to decode data-info (encoding.prefix)"))?;
    let encoding_suffix = buf
        .read_string()
        .ok_or_else(|| zerror!("Failed to decode data-info (encoding.suffix)"))?;
    let encoding_prefix = encoding_prefix
        .try_into()
        .map_err(|_| zerror!("Unknown encoding {}", encoding_prefix))?;
    let encoding = if encoding_suffix.is_empty() {
        Encoding::Exact(encoding_prefix)
    } else {
        Encoding::WithSuffix(encoding_prefix, encoding_suffix.into())
    };
    Ok((encoding, timestamp, deleted))
}

// decode the timestamp only
fn decode_timestamp(buf: &[u8]) -> ZResult<Timestamp> {
    let buf = ZBuf::from(buf.to_vec());
    let mut buf = buf.reader();
    let timestamp = buf
        .read_timestamp()
        .ok_or_else(|| zerror!("Failed to decode data-info (timestamp)"))?;
    Ok(timestamp)
}

// decode the deleted flag only
fn decode_deleted_flag(buf: &[u8]) -> ZResult<bool> {
    let buf = ZBuf::from(buf.to_vec());
    let mut buf = buf.reader();
    let _timestamp = buf
        .read_timestamp()
        .ok_or_else(|| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted = buf
        .read_zint()
        .ok_or_else(|| zerror!("Failed to decode data-info (encoding.prefix)"))?
        != 0;
    Ok(deleted)
}

fn rocksdb_err_to_zerr(err: rocksdb::Error) -> zenoh_core::Error {
    zerror!("Rocksdb error: {}", err.into_string()).into()
}

pub(crate) fn concat_str<S1: AsRef<str>, S2: AsRef<str>>(s1: S1, s2: S2) -> String {
    let mut result = String::with_capacity(s1.as_ref().len() + s2.as_ref().len());
    result.push_str(s1.as_ref());
    result.push_str(s2.as_ref());
    result
}

// Periodic event cleaning-up data info for no-longer existing files
struct GarbageCollectionEvent {
    db: Arc<Mutex<Option<DB>>>,
}

#[async_trait]
impl Timed for GarbageCollectionEvent {
    async fn run(&mut self) {
        trace!("Start garbage collection of obsolete data-infos");
        let time_limit = NTP64::from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
            - *MIN_DELAY_BEFORE_REMOVAL;

        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // prepare a batch with all keys to delete
        let cf_handle = db.cf_handle(CF_DATA_INFO).unwrap();
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for (key, buf) in db.iterator_cf(cf_handle, IteratorMode::Start) {
            if let Ok(true) = decode_deleted_flag(&buf) {
                if let Ok(timestamp) = decode_timestamp(&buf) {
                    if timestamp.get_time() < &time_limit {
                        batch.delete_cf(cf_handle, key);
                        count += 1;
                    }
                }
            }
        }

        // write batch
        if count > 0 {
            trace!("Garbage collect {} old data-info", count);
            if let Err(err) = db.write(batch).map_err(rocksdb_err_to_zerr) {
                warn!("Failed to clean-up old data-info : {}", err);
            }
        }

        trace!("End garbage collection of obsolete data-infos");
    }
}
