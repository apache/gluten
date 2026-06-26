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

import org.apache.gluten.expression.ConverterUtils;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.ExtensionBuilder;
import org.apache.gluten.substrait.type.TypeNode;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.proto.*;
import org.apache.spark.sql.execution.adaptive.InputStats;

import java.util.ArrayList;
import java.util.List;

import scala.math.BigInt;

/**
 * The relation for input iterator, e.g., the input is ColumnarShuffleExchange or RowToColumnarExec.
 * It uses `ReadRel` as the substrait rel type.
 */
public class InputIteratorRelNode implements RelNode {

  private final List<TypeNode> types;
  private final List<String> names;
  private final Long iteratorIndex;
  private BigInt rowSize;

  private InputStats inputStats;

  public InputIteratorRelNode(List<TypeNode> types, List<String> names, Long iteratorIndex) {
    this.types = types;
    this.names = names;
    this.iteratorIndex = iteratorIndex;
  }

  public BigInt getRowSize() {
    return rowSize;
  }

  public void setRowSize(BigInt rowSize) {
    this.rowSize = rowSize;
  }

  public InputStats getInputStats() {
    return inputStats;
  }

  public void setInputStats(InputStats inputStats) {
    this.inputStats = inputStats;
  }

  @Override
  public Rel toProtobuf() {
    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : types) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }

    NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
    nStructBuilder.setStruct(structBuilder.build());
    for (String name : names) {
      nStructBuilder.addNames(name);
    }

    ReadRel.Builder readBuilder = ReadRel.newBuilder();
    readBuilder.setBaseSchema(nStructBuilder.build());

    LocalFilesNode iteratorIndexNode =
        LocalFilesBuilder.makeLocalFiles(ConverterUtils.ITERATOR_PREFIX() + iteratorIndex);
    readBuilder.setLocalFiles(iteratorIndexNode.toProtobuf());

    if (null != rowSize) {
      Any inputRowSize =
          Any.pack(StringValue.newBuilder().setValue("rowSize=" + rowSize.toLong() + "\n").build());
      AdvancedExtensionNode advancedExtension =
          ExtensionBuilder.makeAdvancedExtension(inputRowSize, null);
      readBuilder.setAdvancedExtension(advancedExtension.toProtobuf());
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setRead(readBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNode() {
    return new ArrayList<>();
  }
}
