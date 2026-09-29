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
  void addNativeCompressionParamMapsSupportedParquetCodecs() {
    String[][] compressionCodecs = {
      {"SNAPPY", "snappy"},
      {"GZIP", "gzip"},
      {"zstandard", "zstd"},
      {"LZ4", "lz4"},
      {"LZO", "lzo"},
      {"deflate", "zlib"}
    };

    for (String[] compressionCodec : compressionCodecs) {
      Properties tableProperties = new Properties();
      tableProperties.setProperty("parquet.compression", compressionCodec[0]);

      Map<String, String> tableParams = new HashMap<>();
      tableParams.put("format", "parquet");
      HiveSourceSinkFactory.addNativeCompressionParamFromTableProperties(
          tableProperties, tableParams);

      assertThat(tableParams)
          .containsEntry("sink.file.compression", compressionCodec[1])
          .doesNotContainKey("parquet.compression");
    }
  }

  @Test
  void addNativeCompressionParamReadsSupportedParquetCompressionKeys() {
    Properties tableProperties = new Properties();
    tableProperties.setProperty("parquet.compression.codec", "SNAPPY");

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("format", "parquet");
    HiveSourceSinkFactory.addNativeCompressionParamFromTableProperties(
        tableProperties, tableParams);

    assertThat(tableParams).containsEntry("sink.file.compression", "snappy");
  }

  @Test
  void addNativeCompressionParamDoesNotProduceConfigForUnsupportedFormats() {
    for (String format : new String[] {"orc", "json", "csv", "hive"}) {
      Properties tableProperties = new Properties();
      tableProperties.setProperty("parquet.compression", "SNAPPY");

      Map<String, String> tableParams = new HashMap<>();
      tableParams.put("format", format);
      tableParams.put("sink.file.compression", "snappy");
      HiveSourceSinkFactory.addNativeCompressionParamFromTableProperties(
          tableProperties, tableParams);

      assertThat(tableParams)
          .containsEntry("format", format)
          .doesNotContainKey("sink.file.compression");
    }
  }

  @Test
  void addNativeCompressionParamDoesNotProduceConfigForUnsupportedParquetCodecs() {
    for (String compressionCodec : new String[] {"brotli", "org.example.SnappyCodec"}) {
      Properties tableProperties = new Properties();
      tableProperties.setProperty("parquet.compression", compressionCodec);

      Map<String, String> tableParams = new HashMap<>();
      tableParams.put("format", "parquet");
      HiveSourceSinkFactory.addNativeCompressionParamFromTableProperties(
          tableProperties, tableParams);

      assertThat(tableParams)
          .containsEntry("format", "parquet")
          .doesNotContainKey("sink.file.compression");
    }
  }

  @Test
  void addNativeCompressionParamDoesNotProduceConfigForUnsupportedCompressionKeys() {
    Properties tableProperties = new Properties();
    tableProperties.setProperty("custom.compress", "SNAPPY");
    tableProperties.setProperty("custom.codec", "GZIP");

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put("format", "parquet");
    HiveSourceSinkFactory.addNativeCompressionParamFromTableProperties(
        tableProperties, tableParams);

    assertThat(tableParams)
        .containsEntry("format", "parquet")
        .doesNotContainKey("sink.file.compression");
  }
}
