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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.exception.GlutenNotSupportException;
import org.apache.gluten.expression.WindowFunctionsBuilder;
import org.apache.gluten.substrait.type.TypeBuilder;

import org.apache.spark.sql.catalyst.expressions.CurrentRow$;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WindowFunctionNodeTest {

  private WindowFunctionNode nodeWithLowerBound(Literal lowerBound) {
    return new WindowFunctionNode(
        0,
        Collections.emptyList(),
        "w",
        TypeBuilder.makeFP64(false),
        Literal.create(1L, DataTypes.LongType),
        lowerBound,
        "RANGE",
        false,
        Collections.emptyList());
  }

  @Test
  public void nonIntegralBoundFailsValidation() {
    // Spark casts a RANGE bound to the order key type, so on a decimal key even 10 PRECEDING
    // arrives as -10.00.
    Literal[] bounds = {
      Literal.create(new BigDecimal("-1.50"), DataTypes.createDecimalType(10, 2)),
      Literal.create(new BigDecimal("-10.00"), DataTypes.createDecimalType(10, 2)),
      Literal.create(-0.5d, DataTypes.DoubleType)
    };
    for (Literal bound : bounds) {
      WindowFunctionNode node = nodeWithLowerBound(bound);
      GlutenNotSupportException e = assertThrows(GlutenNotSupportException.class, node::toProtobuf);
      String expected = bound.value() + " (" + bound.dataType().simpleString() + ")";
      assertTrue(e.getMessage(), e.getMessage().endsWith(expected));
    }
  }

  @Test
  public void nullBoundFailsValidation() {
    WindowFunctionNode node = nodeWithLowerBound(Literal.create(null, DataTypes.LongType));
    GlutenNotSupportException e = assertThrows(GlutenNotSupportException.class, node::toProtobuf);
    assertTrue(e.getMessage(), e.getMessage().endsWith("null (bigint)"));
  }

  @Test
  public void longOffsetCheckMatchesConversion() {
    assertFalse(
        WindowFunctionsBuilder.isLongOffset(
            Literal.create(new BigDecimal("-10.00"), DataTypes.createDecimalType(10, 2))));
    assertFalse(WindowFunctionsBuilder.isLongOffset(Literal.create(null, DataTypes.LongType)));
    assertTrue(
        WindowFunctionsBuilder.isLongOffset(
            Literal.create(new BigDecimal("10"), DataTypes.createDecimalType(10, 0))));
    assertTrue(WindowFunctionsBuilder.isLongOffset(Literal.create(-3L, DataTypes.LongType)));
    assertTrue(WindowFunctionsBuilder.isLongOffset(CurrentRow$.MODULE$));
  }

  @Test
  public void rangeLiteralBoundCheckRejectsWhatCannotConvert() {
    // The Velox and Bolt check accepts an integral bound only if it converts, so a null one falls
    // back even with native validation off.
    assertThrows(
        GlutenNotSupportException.class,
        () ->
            WindowFunctionsBuilder.checkRangeFrameLiteralBound(
                Literal.create(null, DataTypes.LongType)));
    assertThrows(
        GlutenNotSupportException.class,
        () ->
            WindowFunctionsBuilder.checkRangeFrameLiteralBound(
                Literal.create(new BigDecimal("-10"), DataTypes.createDecimalType(10, 0))));
    WindowFunctionsBuilder.checkRangeFrameLiteralBound(Literal.create(-3L, DataTypes.LongType));
  }

  @Test
  public void integralBoundConverts() {
    assertNotNull(nodeWithLowerBound(Literal.create(-3L, DataTypes.LongType)).toProtobuf());
  }
}
