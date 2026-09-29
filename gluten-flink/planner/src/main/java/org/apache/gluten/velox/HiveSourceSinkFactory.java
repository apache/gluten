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
  private static final String[] SUPPORTED_COMPRESSION_TABLE_KEYS = {
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
    Object bucketsBuilder = getBucketsBuilder(fileWriterOperator);
    tableParams.putIfAbsent("format", resolveWriteFormat(bucketsBuilder));
    tableParams.put("connector", "hive");
    addHiveCompressionParams(bucketsBuilder, tableParams);
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

  private String resolveWriteFormat(Object bucketsBuilder) {
    String format = resolveFormatFromBucketsBuilder(bucketsBuilder);
    if (format != null) {
      return format;
    }
    return getDefaultFormat();
  }

  @Override
  protected String resolveFormatFromBucketsBuilder(Object bucketsBuilder) {
    Object hiveWriterFactory = getHiveWriterFactoryFromBucketsBuilder(bucketsBuilder);
    if (hiveWriterFactory != null) {
      return resolveFormatFromHiveWriterFactory(hiveWriterFactory);
    }
    return super.resolveFormatFromBucketsBuilder(bucketsBuilder);
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

  private void addHiveCompressionParams(Object bucketsBuilder, Map<String, String> tableParams) {
    Object hiveWriterFactory = getHiveWriterFactoryFromBucketsBuilder(bucketsBuilder);
    if (hiveWriterFactory == null) {
      return;
    }
    Properties tableProperties =
        (Properties) ReflectUtils.tryGetObjectField(hiveWriterFactory, "tableProperties");
    addNativeCompressionParamFromTableProperties(tableProperties, tableParams);
  }

  private Object getBucketsBuilder(OneInputStreamOperator<?, ?> fileWriterOperator) {
    return ReflectUtils.getObjectField(
        ABSTRACT_STREAMING_WRITER_CLASS, fileWriterOperator, "bucketsBuilder");
  }

  private Object getHiveWriterFactoryFromBucketsBuilder(Object bucketsBuilder) {
    Object writerFactory = ReflectUtils.tryGetObjectField(bucketsBuilder, "writerFactory");
    if (writerFactory == null
        || !writerFactory.getClass().getName().contains("HiveBulkWriterFactory")) {
      return null;
    }
    return ReflectUtils.getObjectField(writerFactory.getClass(), writerFactory, "factory");
  }

  static void addNativeCompressionParamFromTableProperties(
      Properties tableProperties, Map<String, String> tableParams) {
    if (!isParquetFormat(tableParams.get("format"))) {
      tableParams.remove(COMPRESSION_KIND);
      return;
    }
    if (tableProperties == null) {
      return;
    }

    String compressionKind = resolveCompressionKind(tableProperties);
    if (compressionKind != null) {
      tableParams.put(COMPRESSION_KIND, compressionKind);
    }
  }

  private static boolean isParquetFormat(String format) {
    return format != null && "parquet".equalsIgnoreCase(format.trim());
  }

  private static String resolveCompressionKind(Properties tableProperties) {
    for (String key : SUPPORTED_COMPRESSION_TABLE_KEYS) {
      String compressionKind = normalizeCompressionKind(tableProperties.getProperty(key));
      if (compressionKind != null) {
        return compressionKind;
      }
    }
    return null;
  }

  static String normalizeCompressionKind(String compression) {
    if (compression == null) {
      return null;
    }
    final Map<String, String> supportedCompressionKinds =
        Map.ofEntries(
            Map.entry("snappy", "snappy"),
            Map.entry("gzip", "gzip"),
            Map.entry("zstd", "zstd"),
            Map.entry("zstandard", "zstd"),
            Map.entry("lz4", "lz4"),
            Map.entry("lzo", "lzo"),
            Map.entry("zlib", "zlib"),
            Map.entry("deflate", "zlib"));
    return supportedCompressionKinds.getOrDefault(compression.trim().toLowerCase(), null);
  }
}
