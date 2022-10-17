<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/master/zenoh-dragon.png" height="150">

[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/vSDSpqnbkm)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# S3 backend for Eclipse zenoh

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This backend relies on [Amazon S3](https://aws.amazon.com/s3/?nc1=h_ls) to implement the storages. It is also compatible to work with [MinIO](https://min.io/) object storage.

Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`zbackend_s3`**.

<!-- :point_right: **Download stable versions:** https://download.eclipse.org/zenoh/zenoh-backend-rocksdb/ -->

:point_right: **Build "master" branch:** see [below](#How-to-build-it)

-------------------------------
## **Examples of usage**

Prerequisites:
 - You have a zenoh router (`zenohd`) installed, and the `zbackend_s3` library file is available in `~/.zenoh/lib`.
 - You have an S3 instance running, this could be an AmazonS3 instance or a MinIO instance.

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API (see https://zenoh.io/docs/manual/plugin-storage-manager/).

**Setting up a MinIO instance**

In order to run the examples of usage from the following section, it is convenient to launch a MinIO instance. To launch MinIO on a Docker container you first, install MinIO with
```
docker pull minio/minio
``` 
And then you can use the following command to launch the instance:
```
docker run -p 9000:9000 -p 9090:9090  --user $(id -u):$(id -g)  --name minio1 -e 'MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE' -e 'MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'  -v ${HOME}/minio/data:/data   quay.io/minio/minio server data --console-address ':9090'"
```

If successful, then the console can be accessed on http://localhost:9090.  
### **Setup via a JSON5 configuration file**

  - Create a `zenoh.json5` configuration file containing:
    ```json5
    {
      plugins: {
        // Configuration of "storage_manager" plugin:
        storage_manager: {
          volumes: {
            s3: {
                // Endpoint where the S3 server is located
                url: "http://localhost:9000",

                private: {
                    // Credentials for interacting with the S3 volume. They may differ from the storage credentials.
                    access_key: "AKIAIOSFODNN7EXAMPLE",
                    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                }
            }
          },
          storages: {
            // Configuration of a "demo" storage using the S3 volume. Each storage is associated to a
            // single S3 bucket.
            s3_storage: {

              // The key expression this storage will subscribes to
              key_expr: "s3/example/*",

              // this prefix will be stripped from the received key when converting to database key.
              // i.e.: "demo/example/a/b" will be stored as "a/b"
              strip_prefix: "s3/example",

              volume: {
                // Id of the volume this storage is associated to
                id: "s3",

                // Bucket to which this storage is associated to
                bucket: "zenoh-bucket",

                // The storage attempts to create the bucket, but if the bucket already exists and is
                // owned by you, then with 'reuse_bucket' you can associate that preexisting bucket to
                // the storage, otherwise it will fail.
                reuse_bucket: true,

                // AWS region to which connect
                region: "eu-west-3",

                // If the storage is read only, it will only handle GET requests
                read_only: false,

                // strategy on storage closure, either `destroy_bucket` or `do_nothing`
                // adminspace.permissions.write needs to be set to true to destroy the bucket on closure
                on_closure: "destroy_bucket",

                private: {
                    // Credentials for interacting with the S3 bucket
                    access_key: "AKIAIOSFODNN7EXAMPLE",
                    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                }
              }
            },
          }
        },
        // Optionally, add the REST plugin
        rest: { http_port: 8000 }
      },
      adminspace: {
        // read and/or write permissions on the admin space
        permissions: {
          read: true,
          write: true,
        },
      },
    }
    ```
  - Run the zenoh router with:  
    ```
    zenohd -c zenoh.json5
    ```

### **Setup at runtime via `curl` commands on the admin space**

  - Run the zenoh router:  
    ```
    cargo run --bin=zenohd -- --adminspace-permissions rw
    ```
  - Add the "s3" backend (the "zbackend_s3" library will be loaded):
    ```
    curl -X PUT -H 'content-type:application/json' -d '{url: "http://localhost:9000", private: {access_key: "AKIAIOSFODNN7EXAMPLE", secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}}' http://localhost:8000/@/router/local/config/plugins/storage_manager/volumes/s3
    ```
  - Add the "s3_storage" storage using the "s3" backend:
    ```
    curl -X PUT -H 'content-type:application/json' -d '{key_expr:"s3/example/*", strip_prefix:"s3/example", volume: {id: "s3", bucket: "zenoh-bucket", create_bucket: true, region: "eu-west-3", on_closure: "do_nothing", private: {access_key: "AKIAIOSFODNN7EXAMPLE", secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}}}' http://localhost:8000/@/router/local/config/plugins/storage_manager/storages/s3_storage
    ```
### **Tests using the REST API**

Using `curl` to publish and query keys/values, you can:
```bash
# Put values that will be stored in the S3 storage
curl -X PUT -H 'content-type:application/json' -d '{"example_key": "example_value"}' http://0.0.0.0:8000/s3/example/test

# To get the stored object
curl -X GET -H {} -d '{}' http://0.0.0.0:8000/s3/example/test

# To delete the previous object
curl -X DELETE -H {} -d '{}' http://0.0.0.0:8000/s3/example/test

# To delete the whole storage (and the bucket if configured)
curl -X DELETE 'http://0.0.0.0:8000/@/router/local/config/plugins/storage_manager/storages/s3_storage'

# To delete the whole volume
curl -X DELETE 'http://0.0.0.0:8000/@/router/local/config/plugins/storage_manager/volumes/s3'
```




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
zenohd v0.6.0-dev-620-g48af2ac3 built with rustc 1.64.0 (a55dd71d5 2022-09-19)
The zenoh router v0.6.0-dev-620-g48af2ac3 built with rustc 1.64.0 (a55dd71d5 2022-09-19)
```

Here, `zenohd` has been built with the rustc version `1.64.0`.  
Install and use this toolchain with the following command:

```bash
$ rustup default 1.64.0
```

And then build the backend with:

```bash
$ cargo build --release --all-targets
```
