---
layout: page
title: Bolt File System Support
nav_order: 11
parent: Getting-Started
---

# Bolt File System Support

This document provides summary of file system support in Bolt.

## 1. Azure ABFS (`abfs`)

Bolt supports connecting to Azure Data Lake Storage Gen2 using the ABFS driver.

- **URI Scheme**: `abfs://<filesystem>@<account>.dfs.core.windows.net/<path>` or `abfss://...` for SSL.
- **Authentication**: Access is configured via a storage account key. Other authentication methods like SAS tokens or service principals are not currently supported by the native Bolt connector.
- **Configuration**: The account key is specified using a configuration property.
  - **Bolt Key**: `fs.azure.account.key.<account>.dfs.core.windows.net`
  - **Spark Equivalent**: In Spark deployments, this is typically configured as `spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net`.

### Example

To access `abfss://myfs@myaccount.dfs.core.windows.net/data/foo.parquet`:

```properties
# In Bolt configuration
fs.azure.account.key.myaccount.dfs.core.windows.net=YOUR_ACCOUNT_KEY
```

## 2. Google Cloud Storage (`gcs`)

Bolt provides a native connector for Google Cloud Storage.

- **URI Scheme**: `gs://<bucket>/<path>`
- **Configuration**: GCS access is configured under the `hive.` prefix.
  - `hive.gcs.scheme`: The protocol to use (`http` or `https`). Defaults to `https`.
  - `hive.gcs.endpoint`: Optional custom endpoint for GCS-compatible storage or mocks.
  - `hive.gcs.credentials`: The JSON string of the service account credentials.

- **Limitations**: The current implementation does not support file system mutation operations: `rename`, `mkdir`, and `rmdir` are not implemented.

### Example

To access `gs://my-bucket/data/bar.orc`:

```properties
# In Bolt configuration
hive.gcs.credentials={"type": "service_account", "project_id": "...", ...}
```

## 3. Amazon S3 & S3-Compatible Storage

Bolt supports Amazon S3 and various S3-compatible object stores like MinIO, Alibaba Cloud OSS, and Tencent Cloud COS.

- **URI Schemes**: Bolt recognizes multiple schemes for S3-compatible access:
  - `s3://`, `s3a://`, `s3n://` (for AWS S3)
  - `oss://` (for Alibaba Cloud OSS)
  - `cos://`, `cosn://` (for Tencent Cloud COS)

- **Configuration**: S3 settings are highly configurable, with global defaults under `hive.s3.` that can be overridden on a per-bucket basis.

  - **Authentication & Endpoint**:
    - `hive.s3.endpoint`: The S3 endpoint URL.
    - `hive.s3.aws-access-key`: AWS Access Key ID.
    - `hive.s3.aws-secret-key`: AWS Secret Access Key.
    - `hive.s3.use-instance-credentials`: Set to `true` to use IAM instance profile credentials.
    - `hive.s3.iam-role`: An IAM role to assume.
    - `hive.s3.iam-role-session-name`: An optional session name for the assumed role.
  - **Behavior**:
    - `hive.s3.path-style-access`: Set to `true` for endpoints that require path-style access (e.g., MinIO). Defaults to `false` (virtual-hosted style).
    - `hive.s3.ssl.enabled`: Set to `true` to use SSL. Defaults to `true`.
  - **Per-Bucket Overrides**: Any global `hive.s3.<key>` can be overridden for a specific bucket using the format `hive.s3.bucket.<BUCKET_NAME>.<key>`.
  - **Optional Controls**:
    - `hive.s3.log-level`: Controls the verbosity of the underlying AWS C++ SDK.
    - `hive.s3.payload-signing-policy`: Configures payload signing (`Always`, `RequestDependent`, `Never`).
    - The connector can also use proxy settings from environment variables.

### Examples

**1. Accessing AWS S3 with IAM credentials:**

```properties
# In Bolt configuration
hive.s3.use-instance-credentials=true
```
Accessing `s3://my-aws-bucket/data/file.parquet`.

**2. Accessing a MinIO-style endpoint:**

```properties
# In Bolt configuration for MinIO
hive.s3.endpoint=http://minio-server:9000
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
```
Accessing `s3://my-minio-bucket/data/file.parquet`.

## 4. HDFS (`hdfs`)

Bolt supports the Hadoop Distributed File System (HDFS).

- **URI Scheme**: `hdfs://<host>:<port>/<path>`
- **Path Handling**: The HDFS connector internally strips the scheme and authority (`<host>:<port>`) from the full URI to derive the file path for HDFS operations.
- **Authentication**: Relies on standard Hadoop client configuration (`core-site.xml`, environment variables) for authentication mechanisms like Kerberos.

### Example

To access `/user/hive/warehouse/table/data.orc` on an HDFS cluster:

```
hdfs://namenode:9000/user/hive/warehouse/table/data.orc
```

## 5. Local File System

Bolt includes a local file system for accessing files on the same machine.

- **URI Scheme**: Handles both absolute file paths and URIs with the `file://` scheme.
- **Operations**: Supports standard file system operations including read, write, create, remove, rename, mkdir, and rmdir.
- **Path Handling**: A path starting with `/` is treated as a local file path. The `file://` prefix is stripped if present.

### Examples

Both of the following paths are valid for accessing a local file:

- `file:///var/data/my-file.txt`
- `/var/data/my-file.txt`

## Notes & Known Limitations

- **GCS**: The GCS file system connector in Bolt does not currently implement `rename`, `mkdir`, or `rmdir`.
- **ABFS**: Authentication is limited to the account key specified in the configuration via a connection string. More advanced authentication methods are not supported in the native connector.
