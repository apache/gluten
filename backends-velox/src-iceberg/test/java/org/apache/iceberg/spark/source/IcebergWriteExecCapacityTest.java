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
package org.apache.iceberg.spark.source;

import org.apache.gluten.execution.IcebergWriteExec;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.SparkWriteConf;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergWriteExecCapacityTest {
  @Test
  public void unitlessTablePropertiesDefaultToBytes() throws Exception {
    for (String value : new String[] {"0", "1024", " 1024 "}) {
      assertTableProperties(value, value.trim() + "B");
    }
  }

  @Test
  public void explicitTablePropertyUnitsArePreserved() throws Exception {
    for (String value : new String[] {"1024B", "1KB", "1MB", "1mb", " 1 MB "}) {
      assertTableProperties(value, value.trim());
    }
  }

  @Test
  public void absentTablePropertiesUseByteDefaults() throws Exception {
    IcebergWriteExec exec = createExec(new HashMap<>(), 8192L);
    assertEquals(
        TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT + "B", exec.getParquetPageSizeBytes());
    assertEquals(TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT + "B", exec.getDictSizeBytes());
  }

  @Test
  public void targetFileSizeUsesBytes() throws Exception {
    for (long value :
        new long[] {0L, 8192L, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT}) {
      assertEquals(value + "B", createExec(new HashMap<>(), value).getTargetFileSizeBytes());
    }
  }

  private void assertTableProperties(String value, String expected) throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.PARQUET_PAGE_SIZE_BYTES, value);
    properties.put(TableProperties.PARQUET_DICT_SIZE_BYTES, value);
    IcebergWriteExec exec = createExec(properties, 8192L);
    assertEquals(expected, exec.getParquetPageSizeBytes());
    assertEquals(expected, exec.getDictSizeBytes());
  }

  private IcebergWriteExec createExec(Map<String, String> properties, long targetFileSize)
      throws Exception {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(properties);
    SparkWriteConf writeConf = mock(SparkWriteConf.class);
    when(writeConf.targetDataFileSize()).thenReturn(targetFileSize);
    SparkWrite write = mock(SparkWrite.class);
    setWriteField(write, "table", table);
    setWriteField(write, "writeConf", writeConf);
    IcebergWriteExec exec = mock(IcebergWriteExec.class, CALLS_REAL_METHODS);
    when(exec.write()).thenReturn(write);
    return exec;
  }

  private void setWriteField(SparkWrite write, String name, Object value) throws Exception {
    Field field = SparkWrite.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(write, value);
  }
}
