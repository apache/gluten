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

public class HiveSourceSinkFactory extends FileSystemSinkFactory {

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
}
