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

import org.apache.gluten.util.ReflectUtils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HiveSourceSinkFactory extends FileSystemSinkFactory {
  private static final String COMPRESSION_KIND = "sink.file.compression";
  private static final String[] TABLE_COMPRESSION_KEYS = {
    "orc.compress", "parquet.compression", "parquet.compression.codec", "parquet.compression-codec"
  };

  @Override
  public boolean match(Transformation<RowData> transformation) {
    if (!isFileSystemSinkTransformation(transformation)) {
      return false;
    }
    return isHiveConnector(transformation);
  }

  @Override
  protected Map<String, String> buildTableParams(
      Object partitionCommitter, OneInputStreamOperator<?, ?> fileWriterOperator) {
    Configuration tableOptions = getTableOptions(partitionCommitter, fileWriterOperator);
    Map<String, String> tableParams = new HashMap<>(tableOptions.toMap());
    tableParams.put("path", getLocationPath(partitionCommitter, fileWriterOperator));
    tableParams.putIfAbsent("format", resolveWriteFormat(fileWriterOperator));
    tableParams.put("connector", "hive");
    addHiveCompressionParams(fileWriterOperator, tableParams);
    return tableParams;
  }

  @Override
  protected String getSinkDescription() {
    return "HiveInsertTable";
  }

  @Override
  protected String getDefaultFormat() {
    return "hive";
  }

  @Override
  protected String resolveFormatFromHadoopBulkWriterFactory(Object writerFactory) {
    Class<?> factoryClass = writerFactory.getClass();
    if (factoryClass.getName().contains("HiveBulkWriterFactory")) {
      Object hiveWriterFactory =
          ReflectUtils.getObjectField(factoryClass, writerFactory, "factory");
      return resolveFormatFromHiveWriterFactory(hiveWriterFactory);
    }
    return super.resolveFormatFromHadoopBulkWriterFactory(writerFactory);
  }

  private String resolveFormatFromHiveWriterFactory(Object hiveWriterFactory) {
    Class<?> factoryClass = hiveWriterFactory.getClass();
    Object serDeInfoCached =
        ReflectUtils.getObjectField(factoryClass, hiveWriterFactory, "serDeInfo");
    Object serDeInfo =
        ReflectUtils.invokeObjectMethod(
            serDeInfoCached.getClass(),
            serDeInfoCached,
            "deserializeValue",
            new Class<?>[] {},
            new Object[] {});
    String serializationLib =
        (String)
            ReflectUtils.invokeObjectMethod(
                serDeInfo.getClass(),
                serDeInfo,
                "getSerializationLib",
                new Class<?>[] {},
                new Object[] {});
    String format = inferFormatFromClassName(serializationLib);
    if (format != null) {
      return format;
    }
    Class<?> outputFormatClz =
        (Class<?>)
            ReflectUtils.getObjectField(factoryClass, hiveWriterFactory, "hiveOutputFormatClz");
    return inferFormatFromClassName(outputFormatClz.getName());
  }

  private void addHiveCompressionParams(
      OneInputStreamOperator<?, ?> fileWriterOperator, Map<String, String> tableParams) {
    Object hiveWriterFactory = getHiveWriterFactory(fileWriterOperator);
    if (hiveWriterFactory == null) {
      return;
    }
    Properties tableProperties =
        (Properties) ReflectUtils.tryGetObjectField(hiveWriterFactory, "tableProperties");
    addCompressionParamsFromTableProperties(tableProperties, tableParams);
  }

  private Object getHiveWriterFactory(OneInputStreamOperator<?, ?> fileWriterOperator) {
    Object bucketsBuilder =
        ReflectUtils.getObjectField(
            ABSTRACT_STREAMING_WRITER_CLASS, fileWriterOperator, "bucketsBuilder");
    Object writerFactory = ReflectUtils.tryGetObjectField(bucketsBuilder, "writerFactory");
    if (writerFactory == null
        || !writerFactory.getClass().getName().contains("HiveBulkWriterFactory")) {
      return null;
    }
    return ReflectUtils.getObjectField(writerFactory.getClass(), writerFactory, "factory");
  }

  static void addCompressionParamsFromTableProperties(
      Properties tableProperties, Map<String, String> tableParams) {
    if (tableProperties == null) {
      return;
    }
    for (String key : tableProperties.stringPropertyNames()) {
      String value = tableProperties.getProperty(key);
      if (isCompressionProperty(key, value)) {
        tableParams.putIfAbsent(key, value);
      }
    }

    String compressionKind = resolveCompressionKind(tableProperties);
    if (compressionKind != null) {
      tableParams.put(COMPRESSION_KIND, compressionKind);
    }
  }

  private static String resolveCompressionKind(Properties tableProperties) {
    for (String key : TABLE_COMPRESSION_KEYS) {
      String compressionKind = normalizeCompressionKind(tableProperties.getProperty(key));
      if (compressionKind != null) {
        return compressionKind;
      }
    }
    return null;
  }

  private static boolean isCompressionProperty(String key, String value) {
    return key != null
        && value != null
        && (key.toLowerCase().contains("compress") || key.toLowerCase().contains("codec"));
  }

  static String normalizeCompressionKind(String compression) {
    if (compression == null) {
      return null;
    }
    String normalized = compression.trim().toLowerCase();
    if (normalized.isEmpty()
        || "none".equals(normalized)
        || "no".equals(normalized)
        || "false".equals(normalized)
        || "uncompressed".equals(normalized)) {
      return null;
    }
    if (normalized.contains("snappy")) {
      return "snappy";
    }
    if (normalized.contains("gzip")) {
      return "gzip";
    }
    if (normalized.contains("zstd") || normalized.contains("zstandard")) {
      return "zstd";
    }
    if (normalized.contains("lz4")) {
      return "lz4";
    }
    if (normalized.contains("lzo")) {
      return "lzo";
    }
    if (normalized.contains("zlib") || normalized.contains("deflate")) {
      return "zlib";
    }
    return normalized;
  }
}
