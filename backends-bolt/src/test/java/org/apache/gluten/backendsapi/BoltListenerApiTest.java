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

import org.apache.gluten.backendsapi.bolt.BoltListenerApi;

import org.apache.spark.SparkConf;
import org.junit.Test;

import scala.collection.immutable.Map;

import static org.junit.Assert.assertEquals;

public class BoltListenerApiTest {

  @Test
  public void testParseByteConfig() {
    SparkConf conf = new SparkConf();
    // Use conf string to prevent BoltConfig object initialization.
    conf.set("spark.gluten.sql.columnar.backend.bolt.filePreloadThreshold", "50MB");

    Map<String, String> parsed = BoltListenerApi.parseConf(conf, false);
    assertEquals(
        "52428800",
        parsed.get("spark.gluten.sql.columnar.backend.bolt.filePreloadThreshold").get());
  }
}
