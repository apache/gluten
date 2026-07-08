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

import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.ExtensionBuilder;
import org.apache.gluten.substrait.type.ColumnTypeNode;
import org.apache.gluten.substrait.type.TypeNode;
import org.apache.gluten.utils.SubstraitUtil;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import org.apache.spark.sql.execution.adaptive.InputStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import scala.math.BigInt;

public class ReadRelNode implements RelNode, Serializable {
  private final List<TypeNode> types = new ArrayList<>();
  private final List<String> names = new ArrayList<>();
  private final List<ColumnTypeNode> columnTypeNodes = new ArrayList<>();
  private final ExpressionNode filterNode;
  private final AdvancedExtensionNode extensionNode;
  private boolean streamKafka = false;

  private BigInt rowCount;
  private InputStats inputStats;

  ReadRelNode(
      List<TypeNode> types,
      List<String> names,
      ExpressionNode filterNode,
      List<ColumnTypeNode> columnTypeNodes,
      AdvancedExtensionNode extensionNode) {
    this.types.addAll(types);
    this.names.addAll(names);
    this.filterNode = filterNode;
    this.columnTypeNodes.addAll(columnTypeNodes);
    this.extensionNode = extensionNode;
  }

  public BigInt getRowCount() {
    return rowCount;
  }

  public void setRowCount(BigInt rowCount) {
    this.rowCount = rowCount;
  }

  public InputStats getInputStats() {
    return inputStats;
  }

  public void setInputStats(InputStats inputStats) {
    this.inputStats = inputStats;
  }

  public void setStreamKafka(boolean streamKafka) {
    this.streamKafka = streamKafka;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    NamedStruct.Builder nStructBuilder =
        SubstraitUtil.createNameStructBuilder(types, names, columnTypeNodes);

    ReadRel.Builder readBuilder = ReadRel.newBuilder();
    readBuilder.setCommon(relCommonBuilder.build());
    readBuilder.setBaseSchema(nStructBuilder.build());
    readBuilder.setStreamKafka(streamKafka);

    if (filterNode != null) {
      readBuilder.setFilter(filterNode.toProtobuf());
    }

    if (extensionNode != null) {
      if (null != rowCount) {
        Any optimization = extensionNode.getOptimization();
        Any enhancement = extensionNode.getEnhancement();
        try {
          String existingOptimizationStr =
              optimization == null ? "" : optimization.unpack(StringValue.class).getValue();
          String newOptimizationStr =
              existingOptimizationStr + "rowSize=" + rowCount.toLong() + "\n";
          Any newOptimization =
              Any.pack(StringValue.newBuilder().setValue(newOptimizationStr).build());
          readBuilder.setAdvancedExtension(
              new AdvancedExtensionNode(newOptimization, enhancement).toProtobuf());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      } else {
        readBuilder.setAdvancedExtension(extensionNode.toProtobuf());
      }
    } else {
      if (null != rowCount) {
        Any inputRowCount =
            Any.pack(
                StringValue.newBuilder().setValue("rowSize=" + rowCount.toLong() + "\n").build());
        AdvancedExtensionNode advancedExtension =
            ExtensionBuilder.makeAdvancedExtension(inputRowCount, null);
        readBuilder.setAdvancedExtension(advancedExtension.toProtobuf());
      }
    }

    Rel.Builder builder = Rel.newBuilder();
    builder.setRead(readBuilder.build());
    return builder.build();
  }

  @Override
  public List<RelNode> childNodes() {
    return new ArrayList<>();
  }
}
