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
package org.apache.gluten.rexnode;

import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RexNodeConverterTest {

  private static FlinkTypeFactory typeFactory;
  private static RexBuilder rexBuilder;

  @BeforeAll
  public static void setUp() {
    typeFactory =
        new FlinkTypeFactory(
            Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  @Test
  public void testVarcharLiteralExtractsRawValue() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral("apple", typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isEqualTo("apple");
  }

  @Test
  public void testCharLiteralExtractsRawValue() {
    RexLiteral literal =
        (RexLiteral)
            rexBuilder.makeLiteral("banana", typeFactory.createSqlType(SqlTypeName.CHAR), false);
    Variant variant = RexNodeConverter.toVariant(literal);
    assertThat(variant).isInstanceOf(VarCharValue.class);
    assertThat(((VarCharValue) variant).getValue()).isEqualTo("banana");
  }
}
