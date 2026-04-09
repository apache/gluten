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
import org.apache.gluten.backendsapi.velox.VeloxListenerApi$;

import org.apache.spark.SparkConf;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import scala.collection.immutable.Map;

import static org.junit.Assert.assertEquals;

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

  @Test
  public void testPlatformLibDirCandidates() {
    List<String> x86Dirs =
        VeloxListenerApi$.MODULE$.platformLibDirCandidatesForTests("Linux", "x86_64");
    assertEquals(Arrays.asList("linux/x86_64", "linux/amd64"), x86Dirs);

    List<String> armDirs =
        VeloxListenerApi$.MODULE$.platformLibDirCandidatesForTests("Mac OS X", "arm64");
    assertEquals(Arrays.asList("darwin/arm64", "darwin/aarch64"), armDirs);
  }
}
