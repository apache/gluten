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
package org.apache.gluten.backendsapi.velox

import org.apache.hadoop.conf.Configuration

import org.scalatest.funsuite.AnyFunSuite

class VeloxBackendSettingsSuite extends AnyFunSuite {

  private def hadoopConf(entries: (String, String)*): Configuration = {
    val conf = new Configuration(false)
    entries.foreach { case (key, value) => conf.set(key, value) }
    conf
  }

  test("detects a Stocator-style cos:// path with a matching endpoint config") {
    val conf = hadoopConf("fs.cos.myservice.endpoint" -> "https://s3.example.com")
    assert(
      VeloxBackendSettings.isIbmCosStocatorPath(
        "cos://mybucket.myservice/warehouse/sample/data/file.parquet",
        conf))
  }

  test("detects a Stocator-style cos:// path with a matching access/secret key config") {
    val conf = hadoopConf("fs.cos.myservice.access.key" -> "AKIA...")
    assert(
      VeloxBackendSettings.isIbmCosStocatorPath("cos://mybucket.myservice/warehouse", conf))

    val conf2 = hadoopConf("fs.cos.myservice.secret.key" -> "secret")
    assert(
      VeloxBackendSettings.isIbmCosStocatorPath("cos://mybucket.myservice/warehouse", conf2))
  }

  test("does not flag a dotted bucket without a corroborating fs.cos.<serviceId>.* config") {
    // A dot alone isn't enough to trigger fallback: it's also valid in a real bucket name.
    val conf = hadoopConf()
    assert(
      !VeloxBackendSettings.isIbmCosStocatorPath("cos://mybucket.myservice/warehouse", conf))
  }

  test("does not flag a cos:// bucket with no dot") {
    val conf = hadoopConf("fs.cos.myservice.endpoint" -> "https://s3.example.com")
    assert(!VeloxBackendSettings.isIbmCosStocatorPath("cos://mybucket/warehouse", conf))
  }

  test("does not flag non-cos schemes even with a dotted authority and matching config") {
    val conf = hadoopConf("fs.cos.myservice.endpoint" -> "https://s3.example.com")
    assert(
      !VeloxBackendSettings.isIbmCosStocatorPath("s3a://mybucket.myservice/warehouse", conf))
  }

  test("does not flag a bucket ending in a dot") {
    val conf = hadoopConf("fs.cos..endpoint" -> "https://s3.example.com")
    assert(!VeloxBackendSettings.isIbmCosStocatorPath("cos://mybucket./warehouse", conf))
  }
}
