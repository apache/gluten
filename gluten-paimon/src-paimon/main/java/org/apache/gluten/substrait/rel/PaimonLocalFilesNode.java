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

import com.google.protobuf.ByteString;
import io.substrait.proto.ReadRel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PaimonLocalFilesNode extends LocalFilesNode {
  private final List<byte[]> serializedSplits;

  public PaimonLocalFilesNode(
      Integer index,
      List<String> paths,
      List<Long> starts,
      List<Long> lengths,
      List<Map<String, String>> partitionColumns,
      ReadFileFormat fileFormat,
      List<String> preferredLocations,
      Map<String, String> properties) {
    super(
        index,
        paths,
        starts,
        lengths,
        new ArrayList<>(),
        new ArrayList<>(),
        partitionColumns,
        new ArrayList<>(),
        fileFormat,
        preferredLocations,
        properties,
        new ArrayList<>());
    this.serializedSplits = null;
  }

  public PaimonLocalFilesNode(
      Integer index, String tablePath, List<byte[]> splits, List<String> preferredLocations) {
    super(
        index,
        java.util.Collections.nCopies(splits.size(), tablePath),
        java.util.Collections.nCopies(splits.size(), 0L),
        java.util.Collections.nCopies(splits.size(), 1L),
        new ArrayList<>(),
        new ArrayList<>(),
        java.util.Collections.nCopies(splits.size(), java.util.Collections.emptyMap()),
        new ArrayList<>(),
        ReadFileFormat.UnknownFormat,
        preferredLocations,
        java.util.Collections.emptyMap(),
        new ArrayList<>());
    this.serializedSplits = splits;
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    if (serializedSplits != null) {
      fileBuilder.setPaimon(
          ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions.newBuilder()
              .setTablePath(getPaths().get(index))
              .setSerializedSplit(ByteString.copyFrom(serializedSplits.get(index)))
              .build());
    }
  }
}
