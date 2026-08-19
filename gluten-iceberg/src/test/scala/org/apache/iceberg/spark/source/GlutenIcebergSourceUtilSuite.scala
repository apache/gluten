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
package org.apache.iceberg.spark.source

import org.scalatest.funsuite.AnyFunSuite

import java.util.{HashMap => JHashMap}

class GlutenIcebergSourceUtilSuite extends AnyFunSuite {

  private val location = "s3://warehouse/ns/db/table"

  test("the vended credential set is extracted with the location and its companions") {
    val ioProperties = new JHashMap[String, String]()
    ioProperties.put("s3.access-key-id", "ASIAVENDED")
    ioProperties.put("s3.secret-access-key", "secret")
    ioProperties.put("s3.session-token", "token")
    ioProperties.put("s3.session-token-expires-at-ms", "1780000000000")
    ioProperties.put("client.region", "us-west-2")
    ioProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

    val extracted = GlutenIcebergSourceUtil.extractVendedReadProperties(ioProperties, location)

    val expected = new JHashMap[String, String]()
    expected.put("s3.access-key-id", "ASIAVENDED")
    expected.put("s3.secret-access-key", "secret")
    expected.put("s3.session-token", "token")
    expected.put("s3.session-token-expires-at-ms", "1780000000000")
    expected.put("client.region", "us-west-2")
    expected.put(GlutenIcebergSourceUtil.LocationKey, location)
    // io-impl is not a credential, so it must not ride along.
    assert(extracted == expected)
  }

  test("a table without vended credentials extracts nothing") {
    val noCredentials = new JHashMap[String, String]()
    noCredentials.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    noCredentials.put("client.region", "us-west-2")
    assert(
      GlutenIcebergSourceUtil.extractVendedReadProperties(noCredentials, location).isEmpty,
      "a region without an access-key/secret pair is not a vended credential set"
    )

    val secretOnly = new JHashMap[String, String]()
    secretOnly.put("s3.secret-access-key", "secret")
    assert(GlutenIcebergSourceUtil.extractVendedReadProperties(secretOnly, location).isEmpty)

    val accessKeyOnly = new JHashMap[String, String]()
    accessKeyOnly.put("s3.access-key-id", "ASIAVENDED")
    assert(GlutenIcebergSourceUtil.extractVendedReadProperties(accessKeyOnly, location).isEmpty)
  }

  test("the session token is optional") {
    val staticKeys = new JHashMap[String, String]()
    staticKeys.put("s3.access-key-id", "AKIASTATIC")
    staticKeys.put("s3.secret-access-key", "secret")

    val expected = new JHashMap[String, String]()
    expected.put("s3.access-key-id", "AKIASTATIC")
    expected.put("s3.secret-access-key", "secret")
    expected.put(GlutenIcebergSourceUtil.LocationKey, location)
    assert(GlutenIcebergSourceUtil.extractVendedReadProperties(staticKeys, location) == expected)
  }
}
