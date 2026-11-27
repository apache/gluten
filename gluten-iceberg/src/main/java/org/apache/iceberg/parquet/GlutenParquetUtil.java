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
package org.apache.iceberg.parquet;

import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class GlutenParquetUtil {
  private static final String PARQUET_INPUT_FILE = "org.apache.parquet.io.InputFile";
  private static final String PARQUET_FILE_READER = "org.apache.parquet.hadoop.ParquetFileReader";

  private GlutenParquetUtil() {}

  /**
   * Returns whether the Parquet footer of {@code file} carries Iceberg field IDs.
   *
   * <p>The Iceberg Spark runtime jar relocates Parquet, while plain Iceberg jars do not. The
   * Parquet classes are therefore resolved from the signatures of the Iceberg build on the
   * classpath instead of being linked against one of the two packages.
   */
  public static boolean hasFieldIds(InputFile file) throws IOException {
    try {
      Method toParquetFile = ParquetIO.class.getDeclaredMethod("file", InputFile.class);
      toParquetFile.setAccessible(true);
      Class<?> parquetInputFile = toParquetFile.getReturnType();
      Class<?> readerClass =
          Class.forName(
              parquetFileReaderClassName(parquetInputFile.getName()),
              true,
              parquetInputFile.getClassLoader());
      Method open = readerClass.getMethod("open", parquetInputFile);
      Method getFileMetaData = readerClass.getMethod("getFileMetaData");
      Method getSchema = getFileMetaData.getReturnType().getMethod("getSchema");
      Method hasIds = ParquetSchemaUtil.class.getMethod("hasIds", getSchema.getReturnType());

      try (AutoCloseable reader =
          (AutoCloseable) invoke(open, null, invoke(toParquetFile, null, file))) {
        Object schema = invoke(getSchema, invoke(getFileMetaData, reader));
        return (Boolean) invoke(hasIds, null, schema);
      }
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Cannot read Parquet field IDs with this Iceberg build", e);
    }
  }

  /**
   * Returns the {@code ParquetFileReader} class name in the same (possibly relocated) package as
   * the Parquet input file class that Iceberg uses.
   */
  static String parquetFileReaderClassName(String parquetInputFileClassName) {
    if (!parquetInputFileClassName.endsWith(PARQUET_INPUT_FILE)) {
      throw new IllegalStateException(
          "Unexpected Parquet input file class " + parquetInputFileClassName);
    }
    String relocationPrefix =
        parquetInputFileClassName.substring(
            0, parquetInputFileClassName.length() - PARQUET_INPUT_FILE.length());
    return relocationPrefix + PARQUET_FILE_READER;
  }

  private static Object invoke(Method method, Object target, Object... args) throws Exception {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw e;
    }
  }
}
