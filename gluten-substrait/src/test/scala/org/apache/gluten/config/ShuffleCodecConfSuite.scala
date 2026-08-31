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

import org.apache.spark.sql.internal.MapProvider

import org.scalatest.funsuite.AnyFunSuiteLike

/**
 * Tests of the shuffle codec conf, which falls back to Spark's `spark.io.compression.codec`.
 *
 * The resolution itself is tested here rather than in `GlutenShuffleUtils`, whose validation needs
 * a loaded backend; see `MiscOperatorSuite` for that side.
 */
class ShuffleCodecConfSuite extends AnyFunSuiteLike {
  private val glutenKey = GlutenConfig.COLUMNAR_SHUFFLE_CODEC.key
  private val sparkKey = GlutenConfig.SPARK_IO_COMPRESSION_CODEC

  private def resolve(conf: (String, String)*): (String, Boolean) = {
    GlutenConfig.COLUMNAR_SHUFFLE_CODEC.readWithSource(new MapProvider(conf.toMap))
  }

  test("neither conf set: Spark's own default applies") {
    assert(GlutenConfig.COLUMNAR_SHUFFLE_CODEC.defaultValueString === "lz4")
    assert(resolve() === (("lz4", false)))
  }

  test("only the Spark conf set: its value is inherited") {
    assert(resolve(sparkKey -> "zstd") === (("zstd", false)))
  }

  test("the Gluten conf wins over the Spark one and is reported as explicitly set") {
    assert(resolve(sparkKey -> "zstd", glutenKey -> "lz4") === (("lz4", true)))
  }

  test("values are lower-cased on both paths") {
    assert(resolve(glutenKey -> "LZ4") === (("lz4", true)))
    assert(resolve(sparkKey -> "ZSTD") === (("zstd", false)))
  }

  test("the fallback key is exposed for error messages") {
    assert(GlutenConfig.COLUMNAR_SHUFFLE_CODEC.fallbackKey === sparkKey)
  }
}
