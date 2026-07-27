---
layout: page
title: Using S3 with Gluten
nav_order: 3
parent: Getting-Started
---
Object stores offered by CSPs such as AWS S3 are important for users of Gluten to store their data. This doc will discuss all details of configs, and use cases around using Gluten with object stores. In order to use an S3 endpoint as your data source, please ensure you are using the following S3 configs in your spark-defaults.conf. If you're experiencing any issues authenticating to S3 with additional auth mechanisms, please reach out to us using the 'Issues' tab.

# Working with S3

## Configuring S3 endpoint

S3 provides the endpoint based method to access the files, here's the example configuration. Users may need to modify some values based on real setup.

```sh
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider    org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key                  XXXXXXXXX
spark.hadoop.fs.s3a.secret.key                  XXXXXXXXX
spark.hadoop.fs.s3a.endpoint                    https://s3.us-west-1.amazonaws.com
spark.hadoop.fs.s3a.connection.ssl.enabled      true
spark.hadoop.fs.s3a.path.style.access           false
```

## Configuring S3 instance credentials

S3 also provides other methods for accessing, you can also use instance credentials by setting the following config

```
spark.hadoop.fs.s3a.use.instance.credentials true
```
Note that in this case, "spark.hadoop.fs.s3a.endpoint" won't take affect as Gluten will use the endpoint set during instance creation.

## Configuring S3 IAM roles
You can also use iam role credentials by setting the following configurations. Instance credentials have higher priority than iam credentials.

```
spark.hadoop.fs.s3a.iam.role  xxxx
spark.hadoop.fs.s3a.iam.role.session.name xxxx
```

Note that `spark.hadoop.fs.s3a.iam.role.session.name` is optional.

## Other authentatication methods are not supported yet

- [AWS temporary credential](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_request.html)

## Log granularity of AWS C++ SDK in velox

You can change log granularity of AWS C++ SDK by setting the `spark.gluten.velox.awsSdkLogLevel` configuration. The Allowed values are:
 "OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE".

## Configuring Whether To Use Proxy From Env for S3 C++ Client
You can change whether to use proxy from env for S3 C++ client by setting the `spark.gluten.velox.s3UseProxyFromEnv` configuration. The Allowed values are:
 "false", "true".

## Configuring S3 Payload Signing Policy
You can change the S3 payload signing policy by setting the `spark.gluten.velox.s3PayloadSigningPolicy` configuration. The Allowed values are:
 "Always", "RequestDependent", "Never".  
- When set to "Always", the payload checksum is included in the signature calculation.  
- When set to "RequestDependent", the payload checksum is included based on the value returned by "AmazonWebServiceRequest::SignBody()".  

## Configuring S3 Log Location
You can set the log location by setting the `spark.gluten.velox.s3LogLocation` configuration.

## Configuring Async S3 Multipart Upload
You can enable asynchronous multipart part upload by setting `spark.gluten.velox.s3UploadPartAsync` to `true`.
Use `spark.gluten.velox.s3MaxConcurrentUploadNum` to control the maximum number of in-flight part uploads per file,
and `spark.gluten.velox.s3UploadThreads` to control the shared upload thread pool size.
These settings apply to all buckets by default. To override a single bucket, use Velox's bucket-specific S3 keys through
Gluten's static backend pass-through prefix, for example
`spark.gluten.velox.hive.s3.bucket.my-bucket.part-upload-async`.

# Local Caching support

Velox supports a local cache when reading data from S3 but not strictly tested and there are several limitations. Please refer [Velox Local Cache](VeloxLocalCache.md) part for more detailed configurations.

# Configurations:

All configurations starts with `spark.hadoop.fs.s3a.`

✅ Supported
❌ Not Supported
⚠️ Partial Support
🔄 In Progress
🚫 Not applied or transparent to Gluten

Here is the list of hadoop s3 file system configurations:

| Name | Default Value | Gluten Honored |
|------|---------------|----------------|
| aws.credentials.provider | (empty) |⚠️|
| security.credential.provider.path | (empty) |❌|
| assumed.role.arn | (empty) |❌|
| assumed.role.session.name | (empty) |❌|
| assumed.role.policy | (empty) |❌|
| assumed.role.session.duration | 30m |❌|
| assumed.role.sts.endpoint | (empty) |❌|
| assumed.role.sts.endpoint.region | (empty) |❌|
| assumed.role.credentials.provider | org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider |❌|
| delegation.token.binding | (empty) |❌|
| attempts.maximum | 5 |❌|
| socket.send.buffer | 8192 |❌|
| socket.recv.buffer | 8192 |❌|
| paging.maximum | 5000 |❌|
| multipart.size | 64M |❌|
| multipart.threshold | 128M |❌|
| multiobjectdelete.enable | true |❌|
| acl.default | (empty) |❌|
| multipart.purge | false |❌|
| multipart.purge.age | 86400 |❌|
| encryption.algorithm | (empty) |❌|
| encryption.key | (empty) |❌|
| signing-algorithm | (empty) |❌|
| block.size | 32M |❌|
| buffer.dir | ${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a |❌|
| fast.upload.buffer | disk |❌|
| fast.upload.active.blocks | 4 |❌|
| readahead.range | 64K |❌|
| user.agent.prefix | (empty) | |
| impl | org.apache.hadoop.fs.s3a.S3AFileSystem |❌|
| retry.limit | 7 |✅|
| retry.interval | 500ms |❌|
| retry.throttle.limit | 20 |❌|
| retry.throttle.interval | 100ms |❌|
| committer.name | file |🚫|
| committer.magic.enabled | true |🚫|
| committer.threads | 8 |🚫|
| committer.staging.tmp.path | tmp/staging |🚫|
| committer.staging.unique-filenames | true |🚫|
| committer.staging.conflict-mode | append |🚫|
| committer.abort.pending.uploads | true |🚫|
| list.version | 2 |🚫|
| etag.checksum.enabled | false |❌|
| change.detection.source | etag |❌|
| change.detection.mode | server |❌|
| change.detection.version.required | true |❌|
| ssl.channel.mode | default_jsse |❌|
| downgrade.syncable.exceptions | true |❌|
| create.checksum.algorithm | (empty) |❌|
| audit.enabled | true |❌|
| vectored.read.min.seek.size|128K|❌|
| vectored.read.max.merged.size|2M|❌|
| vectored.active.ranged.reads|4|❌|
|experimental.input.fadvise|random|❌|
|threads.max|96|❌|
|threads.keepalivetime|60s|❌|
|executor.capacity|16|❌|
|max.total.tasks|16|❌|
| connection.maximum | 25 |✅|
| connection.keepalive | false | ❌ |
| connection.acquisition.timeout | 60s | ❌ |
| connection.establish.timeout | 30s |❌|
| connection.idle.time | 60s | ❌ |
| connection.request.timeout | 60s |❌|
| connection.timeout | 200s |✅|
| connection.ttl | 5m |❌|

Gluten new parameters:
| Name | Default Value | 
|------|---------------|
| access.key | (none) |
| secret.key | (none) |
| endpoint | (none) |
| connection.ssl.enabled | false |
| path.style.access | false |
| retry.limit | (none) |
| retry.mode | legacy |
| instance.credentials | false |
| iam.role | (none) |
| iam.role.session.name | gluten-session |
| endpoint.region | (none) |
| aws.imds.enabled | true |

Gluten configures:
| Name | Default Value | 
|------|---------------|
|spark.gluten.velox.awsSdkLogLevel|FATAL|
|spark.gluten.velox.s3UseProxyFromEnv|false|
|spark.gluten.velox.s3PayloadSigningPolicy|Never|
|spark.gluten.velox.s3LogLocation|(none)|
|spark.gluten.velox.s3UploadPartAsync|false|
|spark.gluten.velox.s3MaxConcurrentUploadNum|4|
|spark.gluten.velox.s3UploadThreads|16|
