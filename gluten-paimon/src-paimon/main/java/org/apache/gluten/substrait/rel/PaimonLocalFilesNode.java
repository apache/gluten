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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PaimonLocalFilesNode extends LocalFilesNode {
  private final List<byte[]> serializedSplits;
  private final String branch;
  private final Map<String, String> readOptions;

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
    this.branch = null;
    this.readOptions = Collections.emptyMap();
  }

  public PaimonLocalFilesNode(
      Integer index,
      String tablePath,
      String branch,
      Map<String, String> readOptions,
      List<byte[]> splits,
      List<String> preferredLocations) {
    super(
        index,
        Collections.nCopies(splits.size(), tablePath),
        Collections.nCopies(splits.size(), 0L),
        Collections.nCopies(splits.size(), 1L),
        new ArrayList<>(),
        new ArrayList<>(),
        Collections.nCopies(splits.size(), Collections.emptyMap()),
        new ArrayList<>(),
        ReadFileFormat.UnknownFormat,
        preferredLocations,
        Collections.emptyMap(),
        new ArrayList<>());
    this.serializedSplits = splits;
    this.branch = branch;
    this.readOptions = readOptions;
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    if (serializedSplits != null) {
      ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions.PaimonCppSplit cppSplit =
          ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions.PaimonCppSplit.newBuilder()
              .setTablePath(getPaths().get(index))
              .setSerializedDataSplit(ByteString.copyFrom(serializedSplits.get(index)))
              .setBranch(branch)
              .putAllReadOptions(readOptions)
              .build();
      fileBuilder.setPaimon(
          ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions.newBuilder()
              .setCppSplit(cppSplit)
              .build());
    }
  }
}
