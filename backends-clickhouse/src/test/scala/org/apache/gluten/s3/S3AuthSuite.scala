/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.s3

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder

import org.apache.hadoop.fs.FileSystem
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off
@Ignore
class S3AuthSuite extends AnyFunSuite {
  val libPath = "/usr/local/clickhouse/lib/libch.so"

  // Throughout this test, the trusted user will visited three buckets:
  // 1. a bucket(trustedOwnedBucket) owned by himself.
  // 2. a bucket(trustingBucket) owned by a trusting user, but he has access to it by assume a role
  // 3. another bucket(trustingBucket2) owned by the same trusting user , but he has access to it by assuming a different role

  // for performance reason, we will use the same parquet file in all three buckets. The parquet file is nation table in tpch10,
  // which is only 3KB

  /** WARNING, the AK/SK should not be committed to github !!!!!!!!!!!!!!! */

  // this is the "trusting user part", e.g. data in customer's bucket
  val trustingBucket = "mhb-dev-bucket" // in us-west-2, @ global dev
  val trustingParquetPath = s"s3a://$trustingBucket/tpch10_nation"
  val trustingEndpoint = "s3.us-west-2.amazonaws.com"
  val trustingBucket2 = "mhb-dev-bucket-us-east-2" // another bucket in us-west-2, @ global dev
  val trustingParquetPath2 = s"s3a://$trustingBucket2/tpch10_nation"
  val trustingEndpoint2 = "s3.us-east-2.amazonaws.com"

  // this is the "trusted user part", e.g. consumer of customer's data
  val trustedAK = System.getenv("S3AuthSuiteAK") // @ global prod
  val trustedSK = System.getenv("S3AuthSuiteSK")
  val trustedOwnedBucket = "mhb-prod-bucket" // another bucket in us-west-2
  val trustedOwnedParquetPath = s"s3a://$trustedOwnedBucket/tpch10_nation"
  val trustedOwnedEndpoint = "s3.us-west-2.amazonaws.com"
  val trustedAssumeRole =
    "arn:aws:iam::429636537981:role/r_mhb_cross_access_to_prod" // -> has access to parquetPath
  val trustedSessionName = "test-session"
  val trustedAssumeRole2 =
    "arn:aws:iam::429636537981:role/r_mhb_cross_access_to_prod2" // -> has access to parquetPath2
  // here external id looks like tested. But it is not. In order to make external id work we must overwrite and
  // modify AssumedRoleCredentialProvider, which is also modified in Kyligence/hadoop-aws. To avoid conflicts
  // we choose not to overwrite AssumedRoleCredentialProvider in Gluten. So without Kyligence/hadoop-aws,
  // external id is NOT supported. So currently trustedAssumeRole2 should NOT have external Id set
  val trustedExternalId2 = "123" // it's just a placeholder !!!
  val trustedSessionName2 = "test-session-2"

  // a separate test case for AWS CN
  val cnEndpoint = "s3.cn-northwest-1.amazonaws.com.cn"
  val cnAK = ""
  val cnSK = ""
  val cnParquetPath = "s3a://mhb-cn-private/test_nation"

  val longRunningAuthModes = List(
    "AKSK" // this requires providing AK/SK of the trusted user
    // "INSTANCE_PROFILE", // this requires running on a EC2 instance, with trusted user's instance profile
    // "PROFILE_FILE" // this requires ~/.aws/credentials configured with the trusted user.
  )
  val enableGlutenOrNot = List(true, false)

  implicit class ImplicitBuilder(val builder: Builder) {

    def withAuthMode(mode: String, assuming: Boolean): Builder = {
      val providerKey =
        if (assuming) "spark.hadoop.fs.s3a.assumed.role.credentials.provider"
        else "spark.hadoop.fs.s3a.aws.credentials.provider"

      mode match {
        case "AKSK" =>
          builder
            .config("spark.hadoop.fs.s3a.access.key", trustedAK)
            .config("spark.hadoop.fs.s3a.secret.key", trustedSK)
            .config(providerKey, "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        case "INSTANCE_PROFILE" =>
          builder.config(providerKey, "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        case "PROFILE_FILE" =>
          builder.config(providerKey, "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        case _ =>
      }
      builder
    }

    def withGluten(enable: Boolean): Builder = {
      builder.config(GlutenConfig.GLUTEN_ENABLED.key, enable.toString)
    }
  }

  for (mode <- longRunningAuthModes; enable <- enableGlutenOrNot) {
    test(s"trusted user authed with mode: $mode, gluten enabled: $enable, assuming role: false") {
      val spark = SparkSession
        .builder()
        .appName("Gluten-S3-Test")
        .master(s"local[1]")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config(GlutenConfig.GLUTEN_LIB_PATH.key, libPath)
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .config("spark.hadoop.fs.s3a.endpoint", trustedOwnedEndpoint)
        // The following two configs are provided to help hadoop-aws to pass.
        // They're not required by native code (they don't have prefix spark.hadoop so
        // native code will not see them)
        // They're also unnecessary in real EC2 instance environment (at least it's the case with Kyligence/hadoop-aws)
        .config("fs.s3a.assumed.role.sts.endpoint.region", "us-east-1")
        .config("fs.s3a.assumed.role.sts.endpoint", "sts.us-east-1.amazonaws.com")
        .withAuthMode(mode, assuming = false)
        .withGluten(enable)
        .enableHiveSupport()
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      try {
        spark.read.parquet(trustedOwnedParquetPath).show(10)
      } finally {
        spark.close
        FileSystem.closeAll()
      }
    }

    test(s"trusted user authed with mode: $mode, gluten enabled: $enable, assuming role: true") {
      val spark = SparkSession
        .builder()
        .appName("Gluten-S3-Test")
        .master(s"local[1]")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config(GlutenConfig.GLUTEN_LIB_PATH.key, libPath)
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .config("spark.hadoop.fs.s3a.endpoint", trustingEndpoint)
        .config(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
        .config("spark.hadoop.fs.s3a.assumed.role.arn", trustedAssumeRole)
        .config("spark.hadoop.fs.s3a.assumed.role.session.name", trustedSessionName)
        // .config("spark.hadoop.fs.s3a.assumed.role.externalId", "") // trustedAssumeRole has no external id
        // The following two configs are provided to help hadoop-aws to pass.
        // They're not required by native code (they don't have prefix spark.hadoop so
        // native code will not see them)
        // They're also unnecessary in real EC2 instance environment (at least it's the case with Kyligence/hadoop-aws)
        .config("fs.s3a.assumed.role.sts.endpoint.region", "us-east-1")
        .config("fs.s3a.assumed.role.sts.endpoint", "sts.us-east-1.amazonaws.com")
        .withAuthMode(mode, assuming = true)
        .withGluten(enable)
        .enableHiveSupport()
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")
      try {
        spark.read.parquet(trustingParquetPath).show(10)
      } finally {
        spark.close
        FileSystem.closeAll()
      }
    }

    test(
      s"trusted user authed with mode: $mode, gluten enabled: $enable, assuming two roles in one session") {
      val spark = SparkSession
        .builder()
        .appName("Gluten-S3-Test")
        .master(s"local[1]")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config(GlutenConfig.GLUTEN_LIB_PATH.key, libPath)
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .config("spark.hadoop.fs.s3a.endpoint", trustingEndpoint2)
        .config(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
        .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket.assumed.role.arn", trustedAssumeRole)
        .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket.assumed.role.externalId", "")
        .config(
          s"spark.hadoop.fs.s3a.bucket.$trustingBucket.assumed.role.session.name",
          trustedSessionName)
        .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket.endpoint", trustingEndpoint)
        .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.arn", trustedAssumeRole2)
        .config(
          s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.session.name",
          trustedSessionName2)
        .config(
          s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.externalId",
          trustedExternalId2
        )
        // The following two configs are provided to help hadoop-aws to pass.
        // They're not required by native code (they don't have prefix spark.hadoop so
        // native code will not see them)
        // They're also unnecessary in real EC2 instance environment (at least it's the case with Kyligence/hadoop-aws)
        .config("fs.s3a.assumed.role.sts.endpoint.region", "us-east-1")
        .config("fs.s3a.assumed.role.sts.endpoint", "sts.us-east-1.amazonaws.com")
        .withAuthMode(mode, assuming = true)
        .withGluten(enable)
        .enableHiveSupport()
        .getOrCreate()

      try {
        spark.read.parquet(trustingParquetPath).show(10)
        spark.read.parquet(trustingParquetPath2).show(10)
      } finally {
        spark.close
        FileSystem.closeAll()
      }
    }
  }

  test(s"test update config later") {
    val spark = SparkSession
      .builder()
      .appName("Gluten-S3-Test")
      .master(s"local[1]")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config(GlutenConfig.GLUTEN_LIB_PATH.key, libPath)
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "1g")
      .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .config("spark.hadoop.fs.s3a.endpoint", trustingEndpoint2)
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
      .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket.assumed.role.arn", trustedAssumeRole)
      .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket.assumed.role.externalId", "")
      .config(
        s"spark.hadoop.fs.s3a.bucket.$trustingBucket.assumed.role.session.name",
        trustedSessionName)
      .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket.endpoint", trustingEndpoint)
//      .config(s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.arn", trustedAssumeRole2)
//      .config(
//        s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.session.name",
//        trustedSessionName2)
//      .config(
//        s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.externalId",
//        trustedExternalId2
//      )
      // The following two configs are provided to help hadoop-aws to pass.
      // They're not required by native code (they don't have prefix spark.hadoop so
      // native code will not see them)
      // They're also unnecessary in real EC2 instance environment (at least it's the case with Kyligence/hadoop-aws)
      .config("fs.s3a.assumed.role.sts.endpoint.region", "us-east-1")
      .config("fs.s3a.assumed.role.sts.endpoint", "sts.us-east-1.amazonaws.com")
      .withAuthMode("AKSK", assuming = true)
      .withGluten(true)
      .enableHiveSupport()
      .getOrCreate()

    try {
      spark.read.parquet(trustingParquetPath).show(10)
      try {
        spark.read.parquet(trustingParquetPath2).show(10)
        throw new Exception("should not reach here")
      } catch {
        case e: java.io.IOException => println("meet io exception as expected")
      }

      // compensate the configs for trustingBucket2
      spark.conf.set(
        s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.arn",
        trustedAssumeRole2)
      spark.conf.set(
        s"spark.hadoop.fs.s3a.bucket.$trustingBucket2.assumed.role.session.name",
        trustedSessionName2)
      // without the following two configs, java side will throw exception.
      // I guess the spark.hadoop.xx configs are only converted to xx once.
      spark.conf.set(s"fs.s3a.bucket.$trustingBucket2.assumed.role.arn", trustedAssumeRole2)
      spark.conf.set(
        s"fs.s3a.bucket.$trustingBucket2.assumed.role.session.name",
        trustedSessionName2)

      spark.read.parquet(trustingParquetPath2).show(10)
    } finally {
      spark.close
      FileSystem.closeAll()
    }
  }

  // a special case for CN aws
  ignore("CN: simple ak sk") {
    val spark = SparkSession
      .builder()
      .appName("Gluten-S3-Test")
      .master(s"local[1]")
      .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .config(GlutenConfig.GLUTEN_LIB_PATH.key, libPath)
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "1g")
      .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
      .config("spark.hadoop.fs.s3a.endpoint", cnEndpoint)
      .config("spark.hadoop.fs.s3a.access.key", cnAK)
      .config("spark.hadoop.fs.s3a.secret.key", cnSK)
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.read.parquet(cnParquetPath).show(10)
    spark.close()
  }

  def close(_spark: SparkSession): Unit = {
    try {
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

}
// scalastyle:on
