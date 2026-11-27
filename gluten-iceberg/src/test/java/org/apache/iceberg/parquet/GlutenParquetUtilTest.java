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

import org.junit.Assert;
import org.junit.Test;

public class GlutenParquetUtilTest {
  @Test
  public void resolvesReaderOfPlainIcebergJars() {
    Assert.assertEquals(
        "org.apache.parquet.hadoop.ParquetFileReader",
        GlutenParquetUtil.parquetFileReaderClassName("org.apache.parquet.io.InputFile"));
  }

  @Test
  public void resolvesReaderOfRelocatedIcebergRuntime() {
    Assert.assertEquals(
        "org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader",
        GlutenParquetUtil.parquetFileReaderClassName(
            "org.apache.iceberg.shaded.org.apache.parquet.io.InputFile"));
  }

  @Test
  public void resolvesReaderOfClasspathIcebergBuild() throws Exception {
    // The class name derived from the Iceberg build on the classpath must be loadable.
    Class<?> inputFile =
        ParquetIO.class
            .getDeclaredMethod("file", org.apache.iceberg.io.InputFile.class)
            .getReturnType();
    Class.forName(
        GlutenParquetUtil.parquetFileReaderClassName(inputFile.getName()),
        false,
        inputFile.getClassLoader());
  }

  @Test(expected = IllegalStateException.class)
  public void rejectsUnexpectedInputFileClass() {
    GlutenParquetUtil.parquetFileReaderClassName("org.example.InputFile");
  }
}
