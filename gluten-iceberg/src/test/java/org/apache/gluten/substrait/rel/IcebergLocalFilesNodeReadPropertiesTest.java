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
package org.apache.gluten.substrait.rel;

import io.substrait.proto.ReadRel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IcebergLocalFilesNodeReadPropertiesTest {

  private static IcebergLocalFilesNode newNode() {
    return new IcebergLocalFilesNode(
        0,
        Collections.singletonList("s3a://warehouse/ns/db/table/data/00000-0-data.parquet"),
        Collections.singletonList(0L),
        Collections.singletonList(100L),
        Collections.singletonList(Collections.emptyMap()),
        LocalFilesNode.ReadFileFormat.ParquetReadFormat,
        Collections.emptyList(),
        Collections.singletonList(Collections.emptyList()),
        Collections.singletonList(Collections.emptyMap()),
        Collections.emptyMap(),
        Collections.emptyMap());
  }

  @Test
  public void serializesTableScopedReadProperties() {
    Map<String, String> readProperties = new HashMap<>();
    readProperties.put("location", "s3://warehouse/ns/db/table");
    readProperties.put("s3.access-key-id", "ASIAVENDED");
    readProperties.put("s3.secret-access-key", "secret");
    readProperties.put("s3.session-token", "token");

    IcebergLocalFilesNode node = newNode();
    node.setReadProperties(readProperties);

    ReadRel.LocalFiles localFiles = node.toProtobuf();

    Assert.assertEquals(readProperties, localFiles.getReadPropertiesMap());
  }

  @Test
  public void omitsReadPropertiesWhenTheTableHasNone() {
    ReadRel.LocalFiles localFiles = newNode().toProtobuf();

    Assert.assertEquals(Collections.emptyMap(), localFiles.getReadPropertiesMap());
  }
}
