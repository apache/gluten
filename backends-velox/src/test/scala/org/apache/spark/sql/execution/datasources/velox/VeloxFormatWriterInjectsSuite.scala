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
package org.apache.spark.sql.execution.datasources.velox

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class VeloxFormatWriterInjectsSuite extends AnyFunSuite {

  private val keyConf = "fs.azure.account.key.account.dfs.core.windows.net"

  private def hadoopConf: Configuration = {
    val conf = new Configuration(false)
    conf.set(keyConf, "the-key")
    conf.set("fs.azure.account.auth.type.account.dfs.core.windows.net", "SharedKey")
    conf.set("fs.s3a.access.key", "not-azure")
    conf
  }

  test("an ABFS write gets the write's Azure settings") {
    val nativeConf = Map("spark.sql.parquet.compression.codec" -> "snappy").asJava
    val conf = VeloxFormatWriterInjects
      .withAzureConf(
        "abfss://container@account.dfs.core.windows.net/t/f.parquet",
        nativeConf,
        hadoopConf)
      .asScala
    assert(
      conf == Map(
        "spark.sql.parquet.compression.codec" -> "snappy",
        s"spark.hadoop.$keyConf" -> "the-key",
        "spark.hadoop.fs.azure.account.auth.type.account.dfs.core.windows.net" -> "SharedKey"
      ))
    // The write's own settings are not changed.
    assert(nativeConf.asScala == Map("spark.sql.parquet.compression.codec" -> "snappy"))
  }

  test("a setting the write already has is kept") {
    val nativeConf = Map(s"spark.hadoop.$keyConf" -> "write-key").asJava
    val conf = VeloxFormatWriterInjects
      .withAzureConf(
        "abfs://container@account.dfs.core.windows.net/f.parquet",
        nativeConf,
        hadoopConf)
    assert(conf.get(s"spark.hadoop.$keyConf") == "write-key")
  }

  test("writes to other file systems are left as they are") {
    val nativeConf = Map("spark.sql.parquet.compression.codec" -> "snappy").asJava
    for (
      path <- Seq("file:/tmp/t/f.parquet", "s3a://bucket/t/f.parquet", "hdfs://nn/t/f.parquet")
    ) {
      assert(VeloxFormatWriterInjects.withAzureConf(path, nativeConf, hadoopConf) eq nativeConf)
    }
  }
}
