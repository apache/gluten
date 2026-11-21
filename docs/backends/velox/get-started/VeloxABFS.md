---
layout: page
title: Using ABFS with Gluten
nav_order: 6
parent: Getting-Started
---
ABFS is an important data store for big data users. This doc discusses config details and use cases of Gluten with ABFS. To use an ABFS account as your data source, please ensure you use the listed ABFS config in your spark-defaults.conf. If you would like to authenticate with ABFS using additional auth mechanisms, please reach out using the 'Issues' tab.

# Working with ABFS

## Configuring ABFS Authentication Type

The authentication mechanism for an Azure storage account is controlled by the following property. Replace `<storage-account>` with the name of your Azure Storage account. Defaults to `SharedKey` if not set.

```sh
spark.hadoop.fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net  SharedKey
```

Allowed values are `SharedKey`, `OAuth`, and `SAS`.

## Configuring ABFS Access Key

To configure access to your storage account using a shared key, replace `<storage-account>` with the name of your account. This property aligns with Spark configurations. By setting this config multiple times using different storage account names, you can access multiple ABFS accounts.

```sh
spark.hadoop.fs.azure.account.key.<storage-account>.dfs.core.windows.net  XXXXXXXXX
```

## Configuring ABFS SAS Token

To authenticate using a pre-generated SAS (Shared Access Signature) token, set the following property. This token provides scoped and time-limited access to specific resources.

```sh
spark.hadoop.fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net  XXXXXXXXX
```

## Configuring ABFS OAuth

To authenticate using OAuth 2.0, set the following properties. Replace `<storage-account>` with the name of your Azure Storage account and `<tenant-id>` with your Azure AD tenant ID.

```sh
spark.hadoop.fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net      XXXXXXXXX
spark.hadoop.fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net  XXXXXXXXX
spark.hadoop.fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net  https://login.microsoftonline.com/<tenant-id>/oauth2/token
```

# Local Caching support

Velox supports a local cache when reading data from ABFS. Please refer [Velox Local Cache](VeloxLocalCache.md) part for more detailed configurations.