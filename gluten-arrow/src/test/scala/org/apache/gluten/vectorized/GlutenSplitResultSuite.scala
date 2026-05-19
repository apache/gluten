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
package org.apache.gluten.vectorized

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[GlutenSplitResult]]'s `customMetrics` reassembly logic. The marshalled form
 * across JNI is two parallel arrays (keys + values); the POJO reassembles them lazily on first
 * `getCustomMetrics()` access and caches an unmodifiable map. These tests exercise the JVM-side
 * boundary without needing a Spark / native-library round-trip.
 */
class GlutenSplitResultSuite extends SparkFunSuite {

  private def newResult(keys: Array[String], values: Array[Long]): GlutenSplitResult = {
    new GlutenSplitResult(
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0L,
      0.0d,
      0L,
      Array.empty[Long],
      Array.empty[Long],
      keys,
      values)
  }

  test("getCustomMetrics returns empty map when native side passed no entries") {
    val r = newResult(Array.empty[String], Array.empty[Long])
    val m = r.getCustomMetrics
    assert(m.isEmpty)
  }

  test("getCustomMetrics returns empty map when native side passed null arrays") {
    val r = newResult(null, null)
    val m = r.getCustomMetrics
    assert(m.isEmpty)
  }

  test("getCustomMetrics reassembles the parallel-array form into a Map") {
    val keys = Array("Velox.InputEncoding.Flat", "Velox.InputEncoding.Dictionary")
    val values = Array(123L, 7L)
    val r = newResult(keys, values)
    val m = r.getCustomMetrics
    assert(m.size() == 2)
    assert(m.get("Velox.InputEncoding.Flat") == 123L)
    assert(m.get("Velox.InputEncoding.Dictionary") == 7L)
  }

  test("getCustomMetrics caches the reassembled map across calls") {
    val r = newResult(Array("k"), Array(1L))
    val first = r.getCustomMetrics
    val second = r.getCustomMetrics
    // Same identity: cached result is returned on subsequent calls.
    assert(first eq second)
  }

  test("getCustomMetrics returns an unmodifiable map") {
    val r = newResult(Array("k"), Array(1L))
    val m = r.getCustomMetrics
    intercept[UnsupportedOperationException] {
      m.put("x", 2L)
    }
  }

  test("getCustomMetrics is null-safe for code reading from older Gluten libs") {
    // Older Gluten native builds may construct GlutenSplitResult without
    // the customMetrics arrays at all — simulate by passing null/null. We
    // already covered that, but assert here that the *empty* map is
    // distinguishable from a populated one.
    val empty = newResult(null, null).getCustomMetrics
    val populated = newResult(Array("a"), Array(1L)).getCustomMetrics
    assert(empty.isEmpty)
    assert(!populated.isEmpty)
  }

  test("getCustomMetrics fails loudly on mismatched key/value array lengths") {
    // A future native-side producer that ships mismatched arrays must not
    // silently corrupt the metrics map (and must not leave the lazy cache
    // field unassigned so subsequent calls re-enter and re-throw). Assert
    // that the first call throws IllegalStateException with both lengths
    // mentioned, and that a second call still throws (the cache field is
    // never assigned to a partial map).
    val r = newResult(Array("a", "b"), Array(1L))
    val ex = intercept[IllegalStateException](r.getCustomMetrics)
    assert(ex.getMessage.contains("2") && ex.getMessage.contains("1"))
    intercept[IllegalStateException](r.getCustomMetrics)
  }

  test("getCustomMetrics fails loudly when values array is null but keys is non-empty") {
    val r = newResult(Array("a"), null)
    val ex = intercept[IllegalStateException](r.getCustomMetrics)
    assert(ex.getMessage.contains("null"))
  }
}
