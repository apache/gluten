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

import org.apache.gluten.substrait.type.TypeNode;
import org.apache.gluten.utils.SubstraitUtil;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class VirtualTableRelNode implements RelNode, Serializable {
  private final List<TypeNode> types;
  private final List<String> names;
  private final List<List<Expression.Literal>> rows;

  VirtualTableRelNode(
      List<TypeNode> types, List<String> names, List<List<Expression.Literal>> rows) {
    Objects.requireNonNull(types, "types");
    Objects.requireNonNull(names, "names");
    Objects.requireNonNull(rows, "rows");
    if (types.size() != names.size()) {
      throw new IllegalArgumentException(
          "Virtual table schema has " + types.size() + " types but " + names.size() + " names.");
    }
    if (rows.isEmpty()) {
      throw new IllegalArgumentException("Virtual table must contain at least one row.");
    }

    this.types = new ArrayList<>(types);
    this.names = new ArrayList<>(names);
    this.rows = new ArrayList<>(rows.size());
    for (List<Expression.Literal> row : rows) {
      Objects.requireNonNull(row, "row");
      if (row.size() != types.size()) {
        throw new IllegalArgumentException(
            "Virtual table row has "
                + row.size()
                + " fields but the schema has "
                + types.size()
                + ".");
      }
      ArrayList<Expression.Literal> rowCopy = new ArrayList<>(row.size());
      for (Expression.Literal literal : row) {
        rowCopy.add(Objects.requireNonNull(literal, "literal"));
      }
      this.rows.add(rowCopy);
    }
  }

  @Override
  public Rel toProtobuf() {
    NamedStruct baseSchema =
        SubstraitUtil.createNameStructBuilder(types, names, Collections.emptyList()).build();

    ReadRel.VirtualTable.Builder virtualTableBuilder = ReadRel.VirtualTable.newBuilder();
    if (types.isEmpty()) {
      // Empty structs carry the row cardinality when there are no fields.
      for (int row = 0; row < rows.size(); ++row) {
        virtualTableBuilder.addExpressions(Expression.Nested.Struct.getDefaultInstance());
      }
    } else {
      Expression.Nested.Struct.Builder rowGroupBuilder = Expression.Nested.Struct.newBuilder();
      for (int column = 0; column < types.size(); ++column) {
        for (List<Expression.Literal> row : rows) {
          rowGroupBuilder.addFields(Expression.newBuilder().setLiteral(row.get(column)).build());
        }
      }
      virtualTableBuilder.addExpressions(rowGroupBuilder.build());
    }

    ReadRel readRel =
        ReadRel.newBuilder()
            .setCommon(RelCommon.newBuilder().setDirect(RelCommon.Direct.newBuilder()).build())
            .setBaseSchema(baseSchema)
            .setVirtualTable(virtualTableBuilder.build())
            .build();
    return Rel.newBuilder().setRead(readRel).build();
  }

  @Override
  public List<RelNode> childNodes() {
    return Collections.emptyList();
  }
}
