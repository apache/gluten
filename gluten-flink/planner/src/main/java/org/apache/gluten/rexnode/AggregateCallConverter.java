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

import io.github.zhztheplayer.velox4j.aggregate.Aggregate;
import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.window.BoundType;
import io.github.zhztheplayer.velox4j.window.Frame;
import io.github.zhztheplayer.velox4j.window.WindowFunction;
import io.github.zhztheplayer.velox4j.window.WindowType;

import org.apache.calcite.rel.core.AggregateCall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Convertor to convert AggregateCall to velox Aggregate */
public class AggregateCallConverter {

  public static Aggregate toAggregate(
      AggregateCall aggregateCall, io.github.zhztheplayer.velox4j.type.RowType inputType) {
    List<Type> rawTypes = new ArrayList<>();
    for (int arg : aggregateCall.getArgList()) {
      rawTypes.add(inputType.getChildren().get(arg));
    }
    CallTypedExpr call = toCall(aggregateCall, inputType);
    return new Aggregate(call, rawTypes, null, List.of(), List.of(), aggregateCall.isDistinct());
  }

  public static List<Aggregate> toAggregates(
      AggregateCall[] aggregateCalls, io.github.zhztheplayer.velox4j.type.RowType inputType) {
    List<Aggregate> aggregates =
        Arrays.stream(aggregateCalls)
            .map(aggregateCall -> toAggregate(aggregateCall, inputType))
            .collect(Collectors.toList());
    return aggregates;
  }

  public static List<Aggregate> toIntermediateAggregates(
      AggregateCall[] aggregateCalls, io.github.zhztheplayer.velox4j.type.RowType inputType) {
    List<Aggregate> aggregates = new ArrayList<>();
    List<TypedExpr> typeExprs = new ArrayList<>();
    for (int i = 0; i < inputType.getNames().size(); i++) {
      typeExprs.add(
          FieldAccessTypedExpr.create(inputType.getChildren().get(i), inputType.getNames().get(i)));
    }
    for (int i = 0; i < aggregateCalls.length; i++) {
      AggregateCall aggregateCall = aggregateCalls[i];
      CallTypedExpr call =
          convertAggregation(
              aggregateCall.getAggregation().getName(),
              typeExprs,
              RexNodeConverter.toType(aggregateCall.getType()));
      aggregates.add(
          new Aggregate(
              call,
              inputType.getChildren(),
              null,
              List.of(),
              List.of(),
              aggregateCall.isDistinct()));
    }
    return aggregates;
  }

  public static WindowFunction toFunction(
      AggregateCall aggregateCall, io.github.zhztheplayer.velox4j.type.RowType inputType) {
    CallTypedExpr call = toCall(aggregateCall, inputType);
    Frame frame =
        new Frame(WindowType.KROWS, BoundType.KCURRENTROW, null, BoundType.KCURRENTROW, null);
    return new WindowFunction(call, frame, false);
  }

  public static List<WindowFunction> toFunctions(
      AggregateCall[] aggregateCalls, io.github.zhztheplayer.velox4j.type.RowType inputType) {
    List<WindowFunction> aggregates =
        Arrays.stream(aggregateCalls)
            .map(aggregateCall -> toFunction(aggregateCall, inputType))
            .collect(Collectors.toList());
    return aggregates;
  }

  private static CallTypedExpr toCall(
      AggregateCall aggregateCall, io.github.zhztheplayer.velox4j.type.RowType inputType) {
    List<TypedExpr> args = new ArrayList<>();
    List<Type> rawTypes = new ArrayList<>();
    for (int arg : aggregateCall.getArgList()) {
      args.add(
          FieldAccessTypedExpr.create(
              inputType.getChildren().get(arg), inputType.getNames().get(arg)));
      rawTypes.add(inputType.getChildren().get(arg));
    }
    return convertAggregation(
        aggregateCall.getAggregation().getName(),
        args,
        RexNodeConverter.toType(aggregateCall.getType()));
  }

  private static CallTypedExpr convertAggregation(String name, List<TypedExpr> args, Type outType) {
    if (name.equals("AVG")) {
      return new CallTypedExpr(new DoubleType(), args, name.toLowerCase());
    } else {
      return new CallTypedExpr(outType, args, name.toLowerCase());
    }
  }
}
