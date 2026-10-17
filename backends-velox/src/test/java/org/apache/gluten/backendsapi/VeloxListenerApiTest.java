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
package org.apache.gluten.backendsapi;

import org.apache.gluten.backendsapi.velox.VeloxListenerApi;

import org.apache.spark.SparkConf;
import org.junit.Test;

import scala.collection.immutable.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class VeloxListenerApiTest {

  @Test
  public void testParseByteConfig() {
    SparkConf conf = new SparkConf();
    // Use conf string to prevent VeloxConfig object initialization.
    conf.set("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold", "50MB");

    Map<String, String> parsed = VeloxListenerApi.parseConf(conf, false);
    assertEquals(
        "52428800",
        parsed.get("spark.gluten.sql.columnar.backend.velox.filePreloadThreshold").get());
  }

  private static final String CACHE_ENABLED =
      "spark.gluten.sql.columnar.backend.velox.cacheEnabled";
  private static final String LOAD_QUANTUM = "spark.gluten.sql.columnar.backend.velox.loadQuantum";

  @Test
  public void testLoadQuantumCappedWhenCacheEnabledByDefault() {
    Map<String, String> parsed = VeloxListenerApi.parseConf(new SparkConf(), false);
    assertEquals("8388608", parsed.get(LOAD_QUANTUM).get());
  }

  @Test
  public void testExplicitLoadQuantumKeptWhenCacheEnabled() {
    SparkConf conf = new SparkConf();
    conf.set(LOAD_QUANTUM, "4MB");

    Map<String, String> parsed = VeloxListenerApi.parseConf(conf, false);
    assertEquals("4194304", parsed.get(LOAD_QUANTUM).get());
  }

  @Test
  public void testLoadQuantumNotSetWhenCacheDisabled() {
    SparkConf conf = new SparkConf();
    conf.set(CACHE_ENABLED, "false");

    Map<String, String> parsed = VeloxListenerApi.parseConf(conf, false);
    assertFalse(parsed.contains(LOAD_QUANTUM));
  }
}
