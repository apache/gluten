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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

import org.scalatest.funsuite.AnyFunSuiteLike

import java.util.TimeZone

import scala.collection.JavaConverters._

/**
 * End-to-end tests of the two conf-passing channels, i.e. what `getNativeSessionConf` and
 * `getNativeBackendConf` actually select out of a given conf map.
 *
 * Note velox/CH conf objects are not on this module's classpath, so only the confs declared by
 * gluten-core and gluten-substrait are registered here.
 */
class NativeConfPassingSuite extends AnyFunSuiteLike {
  private val backendName = "velox"

  private def sessionConf(conf: (String, String)*): Map[String, String] = {
    GlutenConfig.getNativeSessionConf(backendName, conf.toMap)
  }

  private def backendConf(conf: (String, String)*): Map[String, String] = {
    GlutenConfig.getNativeBackendConf(backendName, conf.toMap).asScala.toMap
  }

  test("byte-string values are normalized to bytes for native") {
    val selected = sessionConf(
      GlutenConfig.SPARK_SHUFFLE_FILE_BUFFER -> "32k",
      GlutenConfig.SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE -> "2m",
      GlutenConfig.SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE -> "1024"
    )
    // spark.shuffle.file.buffer is a KiB-unit conf, delivered in bytes.
    assert(selected(GlutenConfig.SPARK_SHUFFLE_FILE_BUFFER) === "32768")
    assert(selected(GlutenConfig.SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE) === "2097152")
    assert(selected(GlutenConfig.SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE) === "1024")
  }

  test("byte-string confs are absent when not set by user") {
    val selected = sessionConf()
    assert(!selected.contains(GlutenConfig.SPARK_SHUFFLE_FILE_BUFFER))
    assert(!selected.contains(GlutenConfig.SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE))
  }

  test("timeParserPolicy is upper-cased for native") {
    val key = SQLConf.LEGACY_TIME_PARSER_POLICY.key
    // Velox compares the value against upper-cased literals. ClickHouse lower-cases it itself, so
    // upper-casing on both channels is safe.
    assert(sessionConf(key -> "legacy")(key) === "LEGACY")
    assert(backendConf(key -> "legacy")(key) === "LEGACY")
  }

  test("a session-mutable foreign conf reaches both channels") {
    val key = SQLConf.LEGACY_SIZE_OF_NULL.key
    assert(sessionConf(key -> "true")(key) === "true")
    assert(backendConf(key -> "true")(key) === "true")
  }

  test("a static foreign conf reaches the backend channel only") {
    val key = SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key
    assert(backendConf(key -> "true")(key) === "true")
    assert(!sessionConf(key -> "true").contains(key))
  }

  test("a session-mutable Gluten conf reaches both channels") {
    val key = GlutenConfig.COLUMNAR_MAX_BATCH_SIZE.key
    assert(sessionConf(key -> "8192")(key) === "8192")
    assert(backendConf(key -> "8192")(key) === "8192")
  }

  test("a static Gluten conf goes to the backend channel only") {
    val key = GlutenConfig.DEBUG_CUDF.key
    assert(backendConf(key -> "true")(key) === "true")
    assert(!sessionConf(key -> "true").contains(key))
  }

  test("passDefault delivers the parsed default when the conf is unset") {
    // A bytes conf whose default string is "64MB" is delivered as a byte count.
    assert(
      sessionConf()(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key) === "67108864")
    // A user-set value wins over the default, and is delivered in parsed form.
    val selected =
      sessionConf(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key -> "32768")
    assert(selected(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key) === "32768")
  }

  test("S3 connection confs carry their default on both channels") {
    // Native has its own fallback for these, and it does not always agree with the value Gluten
    // declares, so the declared default is what both channels deliver.
    Seq(backendConf(), sessionConf()).foreach {
      selected =>
        assert(selected(GlutenConfig.SPARK_S3_CONNECTION_SSL_ENABLED) === "false")
        assert(selected(GlutenConfig.SPARK_S3_PATH_STYLE_ACCESS) === "true")
        assert(selected(GlutenConfig.SPARK_S3_USE_INSTANCE_CREDENTIALS) === "false")
        assert(selected(GlutenConfig.SPARK_S3_RETRY_MAX_ATTEMPTS) === "20")
        assert(selected(GlutenConfig.SPARK_S3_CONNECTION_MAXIMUM) === "15")
    }

    val userSet = sessionConf(GlutenConfig.SPARK_S3_CONNECTION_SSL_ENABLED -> "true")
    assert(userSet(GlutenConfig.SPARK_S3_CONNECTION_SSL_ENABLED) === "true")
  }

  test("the shuffle codec is delivered to native only when set by user") {
    val glutenKey = GlutenConfig.COLUMNAR_SHUFFLE_CODEC.key
    // The fallback to Spark's codec conf is a JVM-side notion: native gets the key only when the
    // user set it, and reads `spark.io.compression.codec` itself otherwise.
    assert(sessionConf(glutenKey -> "lz4")(glutenKey) === "lz4")
    assert(!sessionConf().contains(glutenKey))
  }

  test("the session time zone default follows the JVM default time zone") {
    val key = SQLConf.SESSION_LOCAL_TIMEZONE.key
    // Spark's own default for this key is the current JVM default time zone, which a session - or a
    // test that flips `TimeZone.setDefault` - may change long after Gluten's conf objects were
    // initialized. Native must see the zone in effect now, not the one captured at class load.
    val original = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
      assert(sessionConf()(key) === "America/Los_Angeles")
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))
      assert(sessionConf()(key) === "Asia/Shanghai")
      // A session-set value still wins over the default.
      assert(sessionConf(key -> "UTC")(key) === "UTC")
    } finally {
      TimeZone.setDefault(original)
    }
  }

  test("backend prefix rules are unaffected by the registry") {
    val prefixed = s"${GlutenConfig.prefixOf(backendName)}.someUnknownOption"
    val sessionPrefixed = s"${GlutenConfig.prefixSessionOf(backendName)}.someUnknownOption"
    assert(sessionConf(prefixed -> "v1")(prefixed) === "v1")
    assert(sessionConf(sessionPrefixed -> "v2")(sessionPrefixed) === "v2")
    assert(backendConf(prefixed -> "v1")(prefixed) === "v1")
    assert(backendConf(sessionPrefixed -> "v2")(sessionPrefixed) === "v2")
  }
}
