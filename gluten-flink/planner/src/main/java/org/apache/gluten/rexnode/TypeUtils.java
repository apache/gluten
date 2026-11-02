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

import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.*;

import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TypeUtils {
  private static final List<Class<?>> NUMERIC_TYPE_PRIORITY_LIST =
      List.of(
          TinyIntType.class,
          SmallIntType.class,
          IntegerType.class,
          BigIntType.class,
          RealType.class,
          DoubleType.class);

  private static int getNumericTypePriority(Type type) {
    int index = NUMERIC_TYPE_PRIORITY_LIST.indexOf(type.getClass());
    if (index == -1) {
      // If the type is not found in the list, throw an exception
      throw new RuntimeException("Unsupported type: " + type.getClass().getName());
    }
    return index;
  }

  public static boolean isNumericType(Type type) {
    return NUMERIC_TYPE_PRIORITY_LIST.contains(type.getClass());
  }

  public static boolean isStringType(Type type) {
    return type instanceof VarCharType;
  }

  // Matches the scalar signatures registered under the velox flinksql
  // namespace: booleans, numerics, strings, dates, timestamps and decimals.
  // BINARY, ARRAY, MAP and ROW have no flinksql registration and are
  // rejected at the converter level so conversion fails at planning time
  // instead of failing at runtime.
  public static boolean isScalarType(Type type) {
    return type instanceof BooleanType
        || isNumericType(type)
        || isStringType(type)
        || type instanceof DateType
        || type instanceof TimestampType
        || type instanceof DecimalType;
  }

  public static boolean isIntegerType(Type type) {
    return type instanceof IntegerType;
  }

  public static List<TypedExpr> promoteTypes(List<TypedExpr> exprs) {
    Type targetType = null;
    int targetPriority = -1, targetDecimalPrecision = -1, targetDecimalScale = -1;
    boolean allDecimalType = true;
    for (TypedExpr expr : exprs) {
      Type returnType = expr.getReturnType();
      if (returnType instanceof DecimalType) {
        int precision = ((DecimalType) returnType).getPrecision();
        int scale = ((DecimalType) returnType).getScale();
        if (precision > targetDecimalPrecision) {
          targetDecimalPrecision = precision;
        }
        if (scale > targetDecimalScale) {
          targetDecimalScale = scale;
        }
      } else {
        allDecimalType = false;
        int priority = getNumericTypePriority(returnType);
        if (priority > targetPriority) {
          targetPriority = priority;
          targetType = returnType;
        }
      }
    }
    if (targetType == null && allDecimalType) {
      targetType = new DecimalType(targetDecimalPrecision, targetDecimalScale);
    } else if (targetType == null) {
      throw new FlinkRuntimeException("Logical error, target type can not be null.");
    }
    List<TypedExpr> res = new ArrayList<>();
    for (TypedExpr expr : exprs) {
      Type returnType = expr.getReturnType();
      TypedExpr promotedExpr =
          returnType.equals(targetType)
              ? expr
              : CastTypedExpr.create(
                  targetType,
                  expr instanceof CastTypedExpr ? ((CastTypedExpr) expr).getInputs().get(0) : expr,
                  expr instanceof CastTypedExpr ? ((CastTypedExpr) expr).isNullOnFailure() : false);
      res.add(promotedExpr);
    }
    return res;
  }

  public static List<TypedExpr> promoteTypeForArithmeticExpressions(
      TypedExpr leftExpr, TypedExpr rightExpr) {
    return promoteTypes(Arrays.asList(leftExpr, rightExpr));
  }
}
