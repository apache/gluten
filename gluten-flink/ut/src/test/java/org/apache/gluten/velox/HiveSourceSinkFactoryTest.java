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
package org.apache.gluten.velox;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class HiveSourceSinkFactoryTest {

  @Test
  void addCompressionParamsReadsHiveTableProperties() {
    Properties tableProperties = new Properties();
    tableProperties.setProperty("orc.compress", "SNAPPY");

    Map<String, String> tableParams = new HashMap<>();
    HiveSourceSinkFactory.addCompressionParamsFromTableProperties(tableProperties, tableParams);

    assertThat(tableParams)
        .containsEntry("orc.compress", "SNAPPY")
        .containsEntry("sink.file.compression", "snappy");
  }

  @Test
  void normalizeCompressionKindMapsHiveTablePropertyValues() {
    assertThat(HiveSourceSinkFactory.normalizeCompressionKind("SNAPPY")).isEqualTo("snappy");
    assertThat(HiveSourceSinkFactory.normalizeCompressionKind("GZIP")).isEqualTo("gzip");
    assertThat(HiveSourceSinkFactory.normalizeCompressionKind("zstandard")).isEqualTo("zstd");
    assertThat(HiveSourceSinkFactory.normalizeCompressionKind("UNCOMPRESSED")).isNull();
  }
}
