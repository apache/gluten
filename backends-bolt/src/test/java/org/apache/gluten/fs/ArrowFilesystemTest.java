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
package org.apache.gluten.fs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Primitives;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ArrowFilesystemTest extends TestNativeDataset {

  @ClassRule public static final TemporaryFolder TMP = new TemporaryFolder();

  private void checkParquetReadResult(
      Schema schema, String expectedJson, List<ArrowRecordBatch> actual) throws IOException {
    final ObjectMapper json = new ObjectMapper();
    final Set<?> expectedSet = json.readValue(expectedJson, Set.class);
    final Set<List<Object>> actualSet = new HashSet<>();
    final int fieldCount = schema.getFields().size();
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator())) {
      VectorLoader loader = new VectorLoader(vsr);
      for (ArrowRecordBatch batch : actual) {
        System.out.println(batch.toString());
        loader.load(batch);
        int batchRowCount = vsr.getRowCount();
        for (int i = 0; i < batchRowCount; i++) {
          List<Object> row = new ArrayList<>();
          for (int j = 0; j < fieldCount; j++) {
            Object object = vsr.getVector(j).getObject(i);
            if (Primitives.isWrapperType(object.getClass())) {
              row.add(object);
            } else {
              row.add(object.toString());
            }
          }
          actualSet.add(row);
        }
      }
    }
    Assert.assertEquals(
        "Mismatched data read from Parquet, actual: " + json.writeValueAsString(actualSet) + ";",
        expectedSet,
        actualSet);
  }

  @Test
  public void testBaseCsvRead() throws Exception {
    CsvWriteSupport writeSupport =
        CsvWriteSupport.writeTempFile(
            TMP.newFolder(), "Name,Language", "Juno,Java", "Peter,Python", "Celin,C++");
    String expectedJsonUnordered =
        "[[\"Juno\", \"Java\"], [\"Peter\", \"Python\"], [\"Celin\", \"C++\"]]";
    ScanOptions options = new ScanOptions(100);
    try (FileSystemDatasetFactory factory =
        new FileSystemDatasetFactory(
            rootAllocator(),
            NativeMemoryPool.getDefault(),
            FileFormat.CSV,
            writeSupport.getOutputURI())) {
      List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
      Schema schema = inferResultSchemaFromFactory(factory, options);

      assertScanBatchesProduced(factory, options);
      assertEquals(1, datum.size());
      assertEquals(2, schema.getFields().size());
      assertEquals("Name", schema.getFields().get(0).getName());

      checkParquetReadResult(schema, expectedJsonUnordered, datum);

      AutoCloseables.close(datum);
    }
  }
}
