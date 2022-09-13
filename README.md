<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/master/zenoh-dragon.png" height="150">

[![CI](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/workflows/CI/badge.svg)](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/actions?query=workflow%3A%22CI%22)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/vSDSpqnbkm)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# RocksDB backend for Eclipse zenoh

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on [RocksDB](https://rocksdb.org/) to implement the storages.
Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zbackend_rocksdb`**.

:point_right: **Download stable versions:** https://download.eclipse.org/zenoh/zenoh-backend-rocksdb/

:point_right: **Build "master" branch:** see [below](#How-to-build-it)

-------------------------------
## :warning: Documentation for previous 0.5 versions:
The following documentation related to the version currently in development in "master" branch: 0.6.x.

For previous versions see the README and code of the corresponding tagged version:
 - [0.5.0-beta.9](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/tree/0.5.0-beta.9#readme)
 - [0.5.0-beta.8](https://github.com/eclipse-zenoh/zenoh-backend-rocksdb/tree/0.5.0-beta.8#readme)

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router (`zenohd`) installed, and the `zbackend_rocksdb` library file is available in `~/.zenoh/lib`.
 - Declare the `ZBACKEND_ROCKSDB_ROOT` environment variable to the directory where you want the RocksDB databases
   to be stored. If you don't declare it, the `~/.zenoh/zbackend_rocksdb` directory will be used.

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API.

### **Setup via a JSON5 configuration file**

  - Create a `zenoh.json5` configuration file containing:
    ```json5
    {
      plugins: {
        // configuration of "storages" plugin:
        storage_manager: {
          volumes: {
            // configuration of a "rocksdb" backend (the "zbackend_rocksdb" library will be loaded at startup)
            rocksdb: {}
          },
          storages: {
            // configuration of a "demo" storage using the "rocksdb" backend
            demo: {
              // the key expression this storage will subscribes to
              key_expr: "/demo/example/**",
              // this prefix will be stripped from the received key when converting to database key.
              // i.e.: "/demo/example/a/b" will be stored as "a/b"
              strip_prefix: "/demo/example",
              volume: {
                id: "rocksbd",
              // the RocksDB database will be stored in this directory (relative to ${ZBACKEND_ROCKSDB_ROOT})
                dir: "example",
                // create the RocksDB database if not already existing
                create_db: true
              }
            }
          }
        }
      }
    }
    ```
  - Run the zenoh router with:  
    `zenohd -c zenoh.json5`

### **Setup at runtime via `curl` commands on the admin space**

  - Run the zenoh router:  
    `zenohd`
  - Add the "rocksdb" backend (the "zbackend_rocksdb" library will be loaded):  
   `curl -X PUT -H 'content-type:application/json' -d '{}' http://localhost:8000/@/router/local/config/plugins/storage_manager/volumes/rocksdb`
  - Add the "demo" storage using the "rocksdb" backend:  
   `curl -X PUT -H 'content-type:application/json' -d '{key_expr:"/demo/example/**",strip_prefix:"/demo/example",volume: {id: "rocksbd",dir: "example",create_db: true}}' http://localhost:8000/@/router/local/config/plugins/storage_manager/storages/demo`

### **Tests using the REST API**

Using `curl` to publish and query keys/values, you can:
```bash
# Put values that will be stored in the RocksDB database
curl -X PUT -d "TEST-1" http://localhost:8000/demo/example/test-1
curl -X PUT -d "B" http://localhost:8000/demo/example/a/b

# Retrive the values
curl http://localhost:8000/demo/example/**
```


-------------------------------
## Volume-specific storage configuration
Storages relying on a RocksDB-backed volume must specify some additional configuration as shown [above](#setup-via-a-json5-configuration-file):
- **`"dir"`** (**required**, string) : The name of directory where the RocksDB database is stored.
  The absolute path will be `${ZBACKEND_ROCKSDB_ROOT}/<dir>`.

- **`"create_db"`** (optional, boolean) : create the RocksDB database if not already existing. Not set by default.
  *(the value doesn't matter, only the property existence is checked)*

- **`"read_only"`** (optional, boolean) : the storage will only answer to GET queries. It will not accept any PUT or DELETE message, and won't put anything in RocksDB database. Not set by default. *(the value doesn't matter, only the property existence is checked)*

- **`"on_closure"`** (optional, string) : the strategy to use when the Storage is removed. There are 2 options:
  - *unset*: the database remains untouched (this is the default behaviour)
  - `"destroy_db"`: the database is destroyed (i.e. removed)

-------------------------------
## **Behaviour of the backend**

### Mapping to RocksDB database
Each **storage** will map to a RocksDB database stored in directory: `${ZBACKEND_ROCKSDB_ROOT}/<dir>`, where:
  * `${ZBACKEND_ROCKSDB_ROOT}` is an environment variable that could be specified before zenoh router startup.
     If this variable is not specified `${ZENOH_HOME}/zbackend_rocksdb` will be used
     (where the default value of `${ZENOH_HOME}` is `~/.zenoh`).
  * `<dir>` is the `"dir"` property specified at storage creation.
Each zenoh **key/value** put into the storage will map to 2 **key/values** in the database:
  * For both, the database key is the zenoh key, stripped from the `"strip_prefix"` property specified at storage creation.
  * In the `"default"` [Column Family](https://github.com/facebook/rocksdb/wiki/Column-Families) the key is
    put with the zenoh encoded value as a value.
  * In the `"data_info"` [Column Family](https://github.com/facebook/rocksdb/wiki/Column-Families) the key is
    put with a bytes buffer encoded in this order:
      - the Timestamp encoded as: 8 bytes for the time + 16 bytes for the HLC ID
      - a "is deleted" flag encoded as a boolean on 1 byte
      - the encoding prefix flag encoded as a ZInt (variable length)
      - the encoding suffix encoded as a String (string length as a ZInt + string bytes without ending `\0`)

### Behaviour on deletion
On deletion of a key, the corresponding key is removed from the `"default"` Column Family. An entry with the
"deletion" flag set to true and the deletion timestamp is inserted in the `"data-info"` Column Family
(to avoid re-insertion of points with an older timestamp in case of un-ordered messages).  
At regular interval, a task cleans-up the `"data-info"` Column Family from entries with old timestamps and
the "deletion" flag set to true

### Behaviour on GET
On GET operations:
  * if the selector is a unique key (i.e. not containing any `'*'`): the value and its encoding and timestamp
    for the corresponding key are directly retrieved from the 2 Column Families using `get` RocksDB operation.
  * if the selector is a key expression: the storage searches for matching keys, leveraging RocksDB's [Prefix Seek](https://github.com/facebook/rocksdb/wiki/Prefix-Seek) if possible to minimize the number of entries to check.


-------------------------------
## How to build it

At first, install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). 

:warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
built with the exact same Rust version than `zenohd`. Otherwise, incompatibilities in memory mapping
of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.  
Example:
```bash
$ zenohd --version
The zenoh router v0.6.0-dev-24-g1f20c86 built with rustc 1.57.0 (f1edd0429 2021-11-29)
```
Here, `zenohd` has been built with the rustc version `1.57.0`.  
Install and use this toolchain with the following command:

```bash
$ rustup default 1.57.0
```

And then build the backend with:

```bash
$ cargo build --release --all-targets
```
