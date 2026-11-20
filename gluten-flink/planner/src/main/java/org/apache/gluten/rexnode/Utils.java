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

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.InputTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.join.JoinType;
import io.github.zhztheplayer.velox4j.serializable.ISerializableRegistry;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.variant.VariantRegistry;

import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Utility to store some useful functions. */
public class Utils {

  private static boolean registryInitialized = false;

  // Get names for project node.
  public static List<String> getNamesFromRowType(LogicalType logicalType) {
    if (logicalType instanceof RowType) {
      RowType rowType = (RowType) logicalType;
      return rowType.getFieldNames();
    } else {
      throw new RuntimeException("Output type is not row type: " + logicalType);
    }
  }

  // Init serialize related registries.
  public static void registerRegistry() {
    if (!registryInitialized) {
      registryInitialized = true;
      VariantRegistry.registerAll();
      ISerializableRegistry.registerAll();
    }
  }

  public static JoinType toVLJoinType(FlinkJoinType flinkJoinType) {
    if (flinkJoinType == FlinkJoinType.INNER) {
      return JoinType.INNER;
    } else if (flinkJoinType == FlinkJoinType.LEFT) {
      return JoinType.LEFT;
    } else if (flinkJoinType == FlinkJoinType.RIGHT) {
      return JoinType.RIGHT;
    } else if (flinkJoinType == FlinkJoinType.FULL) {
      return JoinType.FULL;
    } else if (flinkJoinType == FlinkJoinType.ANTI) {
      return JoinType.ANTI;
    } else {
      throw new RuntimeException("Not supported join type: " + flinkJoinType);
    }
  }

  public static List<FieldAccessTypedExpr> analyzeJoinKeys(
      io.github.zhztheplayer.velox4j.type.RowType inputType,
      int[] joinKeys,
      List<int[]> upsertKeys) {
    Set<Integer> joinKeySet = new HashSet();
    Arrays.stream(joinKeys).forEach(joinKeySet::add);
    List<int[]> uniqueKeysContainedByJoinKey =
        upsertKeys.stream()
            .filter(uk -> joinKeySet.containsAll(Arrays.asList(uk)))
            .collect(Collectors.toList());
    List<int[]> uniqueKey = upsertKeys;
    if (!uniqueKeysContainedByJoinKey.isEmpty()) {
      uniqueKey = uniqueKeysContainedByJoinKey;
    } else if (upsertKeys == null || upsertKeys.isEmpty()) {
      uniqueKey = List.of(joinKeys);
    }
    int[] smalleastUniqKeys = uniqueKey.stream().min(Comparator.comparingInt(k -> k.length)).get();
    List<FieldAccessTypedExpr> fieldAccessTypedExprs = new ArrayList<>(smalleastUniqKeys.length);
    for (int i : smalleastUniqKeys) {
      fieldAccessTypedExprs.add(
          FieldAccessTypedExpr.create(new InputTypedExpr(inputType), inputType.getNames().get(i)));
    }
    return fieldAccessTypedExprs;
  }

  public static io.github.zhztheplayer.velox4j.type.RowType substituteSameName(
      io.github.zhztheplayer.velox4j.type.RowType leftInputType,
      io.github.zhztheplayer.velox4j.type.RowType rightInputType,
      io.github.zhztheplayer.velox4j.type.RowType outputType) {
    boolean hasSame = false;
    List<String> newNames = new ArrayList<>(rightInputType.getNames().size());
    for (int i = 0; i < rightInputType.getNames().size(); i++) {
      String name = rightInputType.getNames().get(i);
      if (leftInputType.getNames().contains(name)) {
        hasSame = true;
        newNames.add(outputType.getNames().get(leftInputType.getNames().size() + i));
      } else {
        newNames.add(name);
      }
    }
    if (hasSame) {
      // TODO: this check may not fit all conditions.
      checkArgument(
          outputType.getNames().size()
              == leftInputType.getNames().size() + rightInputType.getNames().size());
      return new io.github.zhztheplayer.velox4j.type.RowType(
          newNames, rightInputType.getChildren());
    }
    return rightInputType;
  }

  public static TypedExpr generateJoinEqualCondition(
      List<FieldAccessTypedExpr> leftKeys, List<FieldAccessTypedExpr> rightKeys) {
    checkArgument(leftKeys.size() == rightKeys.size());
    if (leftKeys.isEmpty()) {
      return null;
    }
    List<TypedExpr> equals =
        IntStream.range(0, leftKeys.size())
            .mapToObj(
                i ->
                    new CallTypedExpr(
                        new BooleanType(), List.of(leftKeys.get(i), rightKeys.get(i)), "equalto"))
            .collect(Collectors.toList());
    return new CallTypedExpr(new BooleanType(), equals, "and");
  }

  public static List<FieldAccessTypedExpr> generateFieldAccesses(
      io.github.zhztheplayer.velox4j.type.RowType inputType, int[] groupings) {
    List<FieldAccessTypedExpr> groupingKeys = new ArrayList<>(groupings.length);
    for (int grouping : groupings) {
      groupingKeys.add(
          FieldAccessTypedExpr.create(
              inputType.getChildren().get(grouping), inputType.getNames().get(grouping)));
    }
    return groupingKeys;
  }

  // Keyless windows (empty grouping): the keyed state backend requires at
  // least one key channel (KeyedStateSpec hard constraint), so append a
  // constant BIGINT column ("$keyless_key", always 0) to the input type
  // and key on the trailing channel; the native operator materializes the
  // column on each runtime batch (the declared type is one column wider
  // than the runtime vectors, and the output projection carries no key
  // column).
  public static io.github.zhztheplayer.velox4j.type.RowType wrapKeylessInput(
      io.github.zhztheplayer.velox4j.type.RowType inputType) {
    List<String> names = new ArrayList<>(inputType.getNames().size() + 1);
    names.addAll(inputType.getNames());
    names.add("$keyless_key");
    List<io.github.zhztheplayer.velox4j.type.Type> children =
        new ArrayList<>(inputType.getChildren().size() + 1);
    children.addAll(inputType.getChildren());
    children.add(new io.github.zhztheplayer.velox4j.type.BigIntType());
    return new io.github.zhztheplayer.velox4j.type.RowType(names, children);
  }
}
