<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/main/zenoh-dragon.png" height="150">

[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/vSDSpqnbkm)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Eclipse Zenoh

The Eclipse Zenoh: Zero Overhead Pub/sub, Store/Query and Compute.

Zenoh (pronounce _/zeno/_) unifies data in motion, data at rest and computations. It carefully blends traditional pub/sub with geo-distributed storages, queries and computations, while retaining a level of time and space efficiency that is well beyond any of the mainstream stacks.

Check the website [zenoh.io](http://zenoh.io) and the [roadmap](https://github.com/eclipse-zenoh/roadmap) for more detailed information.

# S3 backend

In zenoh a backend is a storage technology (such as DBMS, time-series database, file system...) alowing to store the
keys/values publications made via zenoh and return them on queries.
See the [zenoh documentation](https://zenoh.io/docs/manual/plugin-storage-manager/#backends-and-volumes) for more details.

This backend relies on [Amazon S3](https://aws.amazon.com/s3/?nc1=h_ls) to implement the storages. It is also compatible to work with [MinIO](https://min.io/) object storage.

Its library name (without OS specific prefix and extension) that zenoh will rely on to find it and load it is **`libzenoh_backend_s3`**.

:point_right: **Install latest release:** see [below](#how-to-install-it)

:point_right: **Build "main" branch:** see [below](#how-to-build-it)

---

## **Examples of usage**

Prerequisites:

- You have a zenoh router (`zenohd`) installed, and the `libzenoh_backend_s3` library file is available in `~/.zenoh/lib`. Alternatively we can set a symlink to the library, for instance by running:

  ```bash
  ln -s ~/zenoh-backend-s3/target/release/libzenoh_backend_s3.dylib  ~/.zenoh/lib/libzenoh_backend_s3.dylib
  ```

- You have an S3 instance running, this could be an AmazonS3 instance or a MinIO instance.

You can setup storages either at zenoh router startup via a configuration file, either at runtime via the zenoh admin space, using for instance the REST API (see [https://zenoh.io/docs/manual/plugin-storage-manager/](https://zenoh.io/docs/manual/plugin-storage-manager/)).

### Setting up a MinIO instance

In order to run the examples of usage from the following section, it is convenient to launch a MinIO instance. To launch MinIO on a Docker container you first, install MinIO with

```bassh
docker pull minio/minio
```

And then you can use the following command to launch the instance:

```bash
docker run -p 9000:9000 -p 9090:9090  --user $(id -u):$(id -g)  --name minio -e 'MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE' -e 'MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'  -v ${HOME}/minio/data:/data   quay.io/minio/minio server data --console-address ':9090'
```

If successful, then the console can be accessed on [http://localhost:9090](http://localhost:9090).

### **Setup via a JSON5 configuration file**

- Create a `zenoh.json5` configuration file containing:

  ```json5
  {
    plugins: {
      // Configuration of "storage_manager" plugin:
      storage_manager: {
        volumes: {
          s3: {
            // AWS region to which connect (see https://docs.aws.amazon.com/general/latest/gr/s3.html).
            // This field is mandatory if you are going to communicate with an AWS S3 server and
            // optional in case you are working with a MinIO S3 server.
            region: "eu-west-1",

            // Endpoint where the S3 server is located.
            // This parameter allows you to specify a custom endpoint when working with a MinIO S3
            // server.
            // This field is mandatory if you are working with a MinIO server and optional in case
            // you are working with an AWS S3 server as long as you specified the region, in which
            // case the endpoint will be resolved automatically.
            url: "https://s3.eu-west-1.amazonaws.com",

            // Optional TLS specific parameters to enable HTTPS with MinIO. Configuration shared by
            // all the associated storages.
            // tls: {
            //  private: {
            //    // Certificate authority to authenticate the server.
            //    root_ca_certificate_file: "/home/user/certificates/minio/ca.pem",
            //
            //    // Alternatively you can inline your certificate encoded with base 64:
            //    root_ca_certificate_base64: "<YOUR_CERTIFICATE_ENCODED_WITH_BASE64>"
            //  }
            //},
          },
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

              // The storage first checks whether the bucket exists using a read-only 'HeadBucket' call.
              // If the bucket already exists and is accessible, then with 'reuse_bucket' set to true
              // the storage is associated to that preexisting bucket; otherwise (bucket missing) it is created.
              // When 'reuse_bucket' is false, an existing bucket makes the storage creation fail.
              reuse_bucket: true,

              // If the storage is read only, it will only handle GET requests
              read_only: false,

              // strategy on storage closure, either `destroy_bucket` or `do_nothing`
              on_closure: "destroy_bucket",

              // Optional credentials for interacting with the S3 bucket.
              //
              // When this block is omitted, the AWS default credential provider chain is used, as described here:
              // https://docs.rs/aws-config/latest/aws_config/default_provider/credentials/struct.DefaultCredentialsChain.html
              private: {
                access_key: "<YOUR ACCESS KEY>",
                secret_key: "<YOUR SECRET KEY>",
              },
            },
          },
        },
      },
      // Optionally, add the REST plugin
      rest: { http_port: 8000 },
    },
  }
  ```

- Run the zenoh router with:

  ```bash
  zenohd -c zenoh.json5
  ```

#### Volume configuration when working with AWS S3 storage

When working with the AWS S3 storage, the region must be specified following the region names indicated in the [Amazon Simple Storage Service endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/s3.html) documentation. The url of the endpoint is not required as the internal endpoint resolver will automatically
find which one is the endpoint associated to the region specified.

All the storages associated to the volume will use the same region.

The volumes section on the config file will look like:

```json
storage_manager {
  volumes: {
    s3: {
        // AWS region to which connect
        region: "eu-west-1",
    }
  },
  ...
}
```

##### Note on credentials when working with AWS S3 storage

The `private { access_key, secret_key }` block in the storage configuration is **optional**. When it is absent, the backend delegates credential resolution to the [AWS default credential provider chain](https://docs.rs/aws-config/latest/aws_config/default_provider/credentials/struct.DefaultCredentialsChain.html), which is tried in the following order:

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` and `AWS_ACCOUNT_ID`
2. **AWS profile file**: `~/.aws/config` and `~/.aws/credentials`
3. **Web Identity Token**: `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN` (optionally `AWS_ROLE_SESSION_NAME`)
4. **ECS task IAM role**: checked when `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or `AWS_CONTAINER_CREDENTIALS_FULL_URI` is set; ECS/Fargate injects these automatically when a task IAM role is attached
5. **EC2 instance metadata (IMDSv2)**: queried last; provides the IAM role attached to the EC2 instance

Omitting the credentials block is the recommended approach when running on AWS infrastructure (ECS, Fargate, EC2), as it avoids storing long-lived static credentials and relies instead on ephemeral IAM role credentials that rotate automatically.

#### Volume configuration when working with MinIO

Inversely, when working with a MinIO S3 storage, then we need to specify the endpoint of the storage rather than the region, which will be ignored by the MinIO server. We can save ourselves to specify the region in that case.

The volumes section on the config file will look like:

```json
storage_manager {
  volumes: {
    s3: {
        url: "http://localhost:9000",
    }
  },
  ...
}
```

### **Setup at runtime via `curl` commands on the admin space**

- Run the zenoh router:

  ```bash
  cargo run --bin=zenohd
  ```

- Add the "s3" backend (the "zenoh_backend_s3" library will be loaded):

  ```bash
  curl -X PUT -H 'content-type:application/json' -d '{url: "http://localhost:9000", private: {access_key: "AKIAIOSFODNN7EXAMPLE", secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}}' http://localhost:8000/@/local/router/config/plugins/storage_manager/volumes/s3
  ```

- Add the "s3_storage" storage using the "s3" backend:

  ```bash
  curl -X PUT -H 'content-type:application/json' -d '{key_expr:"s3/example/*", strip_prefix:"s3/example", volume: {id: "s3", bucket: "zenoh-bucket", create_bucket: true, region: "eu-west-3", on_closure: "do_nothing", private: {access_key: "AKIAIOSFODNN7EXAMPLE", secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}}}' http://localhost:8000/@/local/router/config/plugins/storage_manager/storages/s3_storage
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

# To delete the whole storage and the bucket if configured (note in order for this test to work, you need to setup adminspace read/write permissions)
curl -X DELETE 'http://0.0.0.0:8000/@/local/router/config/plugins/storage_manager/storages/s3_storage'

# To delete the whole volume (note in order for this test to work, you need to setup adminspace read/write permissions)
curl -X DELETE 'http://0.0.0.0:8000/@/local/router/config/plugins/storage_manager/volumes/s3'
```

## **Enabling TLS on MinIO**

In order to establish secure communication through HTTPS we need to provide a certificate of the certificate authority that validates the server credentials.

TLS certificates can be generated as explained in the [zenoh documentation using Minica](https://zenoh.io/docs/manual/tls/). When running

```bash
minica --domains localhost
```

a private key, a public certificate and a certificate authority certificate is generated:

```raw
└── certificates
    ├── localhost
    │   ├── cert.pem
    │   └── key.pem
    ├── minica-key.pem
    └── minica.pem
```

On the config file, we need to specify the `root_ca_certificate_file` as this will allow the s3 plugin to validate the MinIO server keys.
Example:

```json
tls: {
  private: {
    root_ca_certificate_file: "/home/user/certificates/minio/minica.pem",
  },
},
```

Here, the `root_ca_certificate_file` corresponds to the generated _minica.pem_ file.
You can also embed directly the root_ca_certificate by inlining it under the filed `root_ca_certificate_base64`, encoded with base64.

The _cert.pem_ and _key.pem_ files correspond to the public certificate and private key respectively. We need to rename them as _public.crt_ and _private.key_ respectively and store them under the MinIO configuration directory (as specified in the [MinIO documentation](https://min.io/docs/minio/linux/operations/network-encryption.html#enabling-tls)). In case you are using running a docker container as previously shown, then we will need to mount the folder containing the certificates as a volume; supposing we stored our certificates under `${HOME}/minio/certs`, we need to start our container as follows:

```bash
docker run -p 9000:9000 -p 9090:9090  --user $(id -u):$(id -g)  --name minio -e 'MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE' -e 'MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' -v ${HOME}/minio/data:/data -v ${HOME}/minio/certs:/certs quay.io/minio/minio server data --certs-dir certs --console-address ':9090'
```

Finally the volume configuration should then look like:

```json
storage_manager: {
  volumes: {
    s3: {
        // Endpoint where the S3 server is located
        url: "https://localhost:9000",

        // Configure TLS specific parameters
        tls: {
          private: {
            root_ca_certificate_file: "/home/user/certificates/minio_certs/minica.pem",
          },
        },
    },
  },
```

_Note: do not forget to modify the endpoint protocol, for instance from `http://localhost:9090` to `https://localhost:9090`_

---

## How to install it

To install the latest release of this backend library, you can do as follows:

### Manual installation (all platforms)

All release packages can be downloaded from:

- [https://download.eclipse.org/zenoh/zenoh-backend-s3/latest/](https://download.eclipse.org/zenoh/zenoh-backend-s3/latest/)

Each subdirectory has the name of the Rust target. See the platforms each target corresponds to on [https://doc.rust-lang.org/stable/rustc/platform-support.html](https://doc.rust-lang.org/stable/rustc/platform-support.html)

Choose your platform and download the `.zip` file.  
Unzip it in the same directory than `zenohd` or to any directory where it can find the backend library (e.g. /usr/lib or ~/.zenoh/lib)

### Linux Debian

Add Eclipse Zenoh private repository to the sources list, and install the `zenoh-backend-s3` package:

```bash
echo "deb [trusted=yes] https://download.eclipse.org/zenoh/debian-repo/ /" | sudo tee -a /etc/apt/sources.list > /dev/null
sudo apt update
sudo apt install zenoh-backend-s3
```

---

## How to build it

At first, install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). If you already have the Rust toolchain installed, make sure it is up-to-date with:

```bash
rustup update
```

> :warning: **WARNING** :warning: : As Rust doesn't have a stable ABI, the backend library should be
> built with the exact same Rust version than `zenohd`. Otherwise, incompatibilities in memory mapping
> of shared types between `zenohd` and the library can lead to a `"SIGSEV"` crash.

To know the Rust version you're `zenohd` has been built with, use the `--version` option.  
Example:

```bash
$ zenohd --version
zenohd v0.7.0-rc-365-geca888b4-modified built with rustc 1.69.0 (84c898d65 2023-04-16)
The zenoh router v0.7.0-rc-365-geca888b4-modified built with rustc 1.69.0 (84c898d65 2023-04-16)
```

Here, `zenohd` has been built with the rustc version `1.69.0`.
Install and use this toolchain with the following command:

```bash
rustup default 1.69.0
```

And then build the backend with:

```bash
cargo build --release --all-targets
```
