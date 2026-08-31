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

import org.apache.spark.network.util.ByteUnit
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

  test("byte-string values are delivered in the conf's declared unit for native") {
    val selected = sessionConf(
      GlutenConfig.SPARK_SHUFFLE_FILE_BUFFER -> "32k",
      GlutenConfig.SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE -> "2m",
      GlutenConfig.SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE -> "1024"
    )
    // spark.shuffle.file.buffer is a KiB-unit conf, matching Spark's own declaration, so it is
    // delivered as the KiB count; native multiplies by 1024 to get bytes.
    assert(selected(GlutenConfig.SPARK_SHUFFLE_FILE_BUFFER) === "32")
    // The other two are BYTE-unit confs, so they are delivered as byte counts.
    assert(selected(GlutenConfig.SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE) === "2097152")
    assert(selected(GlutenConfig.SPARK_SHUFFLE_SPILL_DISK_WRITE_BUFFER_SIZE) === "1024")
  }

  test("an unset byte-string conf is not delivered when declared createOptional") {
    val selected = sessionConf()
    // `createOptional` means nothing is delivered when unset - native's own fallback applies.
    assert(!selected.contains(GlutenConfig.SPARK_SHUFFLE_FILE_BUFFER))
    assert(!selected.contains(GlutenConfig.SPARK_UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE))
  }

  test("timeParserPolicy is upper-cased for native") {
    val key = SQLConf.LEGACY_TIME_PARSER_POLICY.key
    // Velox compares the value against upper-cased literals. ClickHouse lower-cases it itself, so
    // upper-casing on both channels is safe.
    assert(sessionConf(key -> "legacy")(key) === "LEGACY")
    assert(backendConf(key -> "legacy")(key) === "LEGACY")
    // Declared `createOptional`, so nothing is delivered when unset (native's fallback "" works
    // because absent == non-LEGACY, which is what EXCEPTION semantics require).
    assert(!sessionConf().contains(key))
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

  test("a Gluten conf's own default is delivered when unset, in parsed form") {
    // A bytes conf whose default string is "64MB" is delivered as a byte count.
    assert(
      sessionConf()(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key) === "67108864")
    // A user-set value wins over the default, and is delivered in parsed form.
    val selected =
      sessionConf(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key -> "32768")
    assert(selected(GlutenConfig.GLUTEN_COLUMNAR_TO_ROW_MEM_THRESHOLD.key) === "32768")
  }

  test("a Spark conf's default is taken from Spark's own declaration") {
    // `createOptional`: native's own fallback matches Spark's default, so nothing is delivered
    // when unset.
    Seq(
      SQLConf.CASE_SENSITIVE,
      SQLConf.IGNORE_MISSING_FILES,
      SQLConf.LEGACY_STATISTICAL_AGGREGATE,
      SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS
    ).foreach(e => assert(!sessionConf().contains(e.key)))
    assert(!sessionConf().contains(GlutenConfig.SPARK_SHUFFLE_SPILL_COMPRESS))
    // `createWithForeignDefault`: native's own fallback is wrong, so Spark's declared default is
    // delivered, resolved from Spark's entry rather than restated - a restated default is exactly
    // the drift this mechanism removes, and these two differ across Spark versions.
    Seq(SQLConf.MAP_KEY_DEDUP_POLICY, SQLConf.ANSI_ENABLED)
      .foreach(e => assert(sessionConf()(e.key) === e.defaultValueString))
    // A user-set value still wins.
    assert(sessionConf(SQLConf.CASE_SENSITIVE.key -> "true")(SQLConf.CASE_SENSITIVE.key) === "true")
  }

  test("a conf with no declared default anywhere is delivered only when set") {
    // A Hadoop key that no Spark entry declares: native's own handling of an absent key applies,
    // which for the S3 credentials is what tells it no credentials were configured.
    assert(!sessionConf().contains(GlutenConfig.SPARK_S3_ACCESS_KEY))
    assert(
      sessionConf(GlutenConfig.SPARK_S3_ACCESS_KEY -> "ak")(
        GlutenConfig.SPARK_S3_ACCESS_KEY) === "ak")
    // A Gluten conf declared with `createOptional`, for the same reason - `enableDumping` checks
    // whether the key is present at all.
    assert(!sessionConf().contains(GlutenConfig.BENCHMARK_SAVE_DIR.key))
  }

  test("a Gluten-side default is declared only where it departs from Hadoop's") {
    Seq(backendConf(), sessionConf()).foreach {
      selected =>
        // No Spark entry declares these Hadoop keys, so Gluten declares the default itself where
        // its choice departs from what native falls back to: `ConfigExtractor` falls back to
        // `false` for path.style.access and `25` for connection.maximum, and has no fallback for
        // retry.limit.
        assert(selected(GlutenConfig.SPARK_S3_PATH_STYLE_ACCESS) === "true")
        assert(selected(GlutenConfig.SPARK_S3_RETRY_MAX_ATTEMPTS) === "20")
        assert(selected(GlutenConfig.SPARK_S3_CONNECTION_MAXIMUM) === "15")
        // Native's fallback already agrees, so no default is declared and nothing is delivered.
        assert(!selected.contains(GlutenConfig.SPARK_S3_CONNECTION_SSL_ENABLED))
        assert(!selected.contains(GlutenConfig.SPARK_S3_USE_INSTANCE_CREDENTIALS))
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

  test("a declared key wins over a prefix rule that also matches it") {
    // A key can be both declared with `passToNative` and matched by a prefix rule. The registry
    // selection runs first, so the prefix rule must not overwrite it - only the declaration applies
    // the entry's value converter, and overwriting would silently deliver the raw value instead.
    val key = s"${GlutenConfig.prefixSessionOf(backendName)}.declaredWithTransform"
    try {
      PrefixOverlapTestConfig.declare(key)
      assert(sessionConf(key -> "64k")(key) === "65536")
      assert(backendConf(key -> "64k")(key) === "65536")
    } finally {
      NativeConfRegistry.unregister(key)
    }
  }

  /** Declares a conf whose key is matched by a prefix rule as well as by the registry. */
  private object PrefixOverlapTestConfig extends ConfigRegistry {
    override def get: GlutenCoreConfig = GlutenCoreConfig.get

    def declare(key: String): Unit =
      registerConf(key)
        .bytesConf(ByteUnit.BYTE)
        .passToNative()
        .createOptional
  }
}
