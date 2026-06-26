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

import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.type.ColumnTypeNode;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.gluten.utils.SubstraitUtil;

import io.substrait.proto.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WriteRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final List<TypeNode> types = new ArrayList<>();
  private final List<String> names = new ArrayList<>();

  private final List<ColumnTypeNode> columnTypeNodes = new ArrayList<>();

  private final AdvancedExtensionNode extensionNode;

  private final WriteRel.BucketSpec bucketSpec;

  WriteRelNode(
      RelNode input,
      List<TypeNode> types,
      List<String> names,
      List<ColumnTypeNode> partitionColumnTypeNodes,
      AdvancedExtensionNode extensionNode,
      WriteRel.BucketSpec bucketSpec) {
    this.input = input;
    this.types.addAll(types);
    this.names.addAll(names);
    this.columnTypeNodes.addAll(partitionColumnTypeNodes);
    this.extensionNode = extensionNode;
    this.bucketSpec = bucketSpec;
  }

  @Override
  public Rel toProtobuf() {

    WriteRel.Builder writeBuilder = WriteRel.newBuilder();

    NamedStruct.Builder nStructBuilder =
        SubstraitUtil.createNameStructBuilder(types, names, columnTypeNodes);

    writeBuilder.setTableSchema(nStructBuilder);

    NamedObjectWrite.Builder nameObjectWriter = NamedObjectWrite.newBuilder();

    if (extensionNode != null) {
      nameObjectWriter.setAdvancedExtension(extensionNode.toProtobuf());
    }

    if (bucketSpec != null) {
      writeBuilder.setBucketSpec(bucketSpec);
    }

    writeBuilder.setNamedTable(nameObjectWriter);

    if (input != null) {
      writeBuilder.setInput(input.toProtobuf());
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setWrite(writeBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return Collections.singletonList(input);
  }
}
