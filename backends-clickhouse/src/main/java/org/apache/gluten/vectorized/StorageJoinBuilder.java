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
package org.apache.gluten.vectorized;

import org.apache.gluten.execution.BroadcastJoinContext;
import org.apache.gluten.execution.JoinTypeTransform;
import org.apache.gluten.expression.ConverterUtils$;
import org.apache.gluten.utils.SubstraitUtil;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

public class StorageJoinBuilder {

  public static native void nativeCleanBuildHashTable(String hashTableId, long hashTableData);

  public static native long nativeCloneBuildHashTable(long hashTableData);

  private static native long nativeBuild(
      String buildTableId,
      byte[] in,
      long rowCount,
      String joinKeys,
      int joinType,
      boolean isBhj,
      boolean hasMixedFiltCondition,
      boolean isExistenceJoin,
      byte[] namedStruct,
      boolean isNullAwareAntiJoin,
      boolean hasNullKeyValues);

  private StorageJoinBuilder() {}

  /** build storage join object */
  public static long build(
      byte[] batches,
      long rowCount,
      BroadcastJoinContext broadcastContext,
      List<Expression> newBuildKeys,
      List<Attribute> newOutput,
      boolean hasNullKeyValues) {
    ConverterUtils$ converter = ConverterUtils$.MODULE$;
    List<Expression> keys;
    List<Attribute> output;
    if (newBuildKeys.isEmpty()) {
      keys = JavaConverters.<Expression>seqAsJavaList(broadcastContext.buildSideJoinKeys());
      output = JavaConverters.<Attribute>seqAsJavaList(broadcastContext.buildSideStructure());
    } else {
      keys = newBuildKeys;
      output = newOutput;
    }
    String joinKey =
        keys.stream()
            .map(
                (Expression key) -> {
                  Attribute attr = converter.getAttrFromExpr(key);
                  return converter.genColumnNameWithExprId(attr);
                })
            .collect(Collectors.joining(","));

    int joinType;
    if (broadcastContext.isBhj()) {
      boolean buildRight = broadcastContext.buildRight();
      joinType =
          JoinTypeTransform.toSubstraitJoinType(broadcastContext.joinType(), buildRight).ordinal();
    } else {
      joinType = SubstraitUtil.toCrossRelSubstrait(broadcastContext.joinType()).ordinal();
    }

    return nativeBuild(
        broadcastContext.buildTableId(),
        batches,
        rowCount,
        joinKey,
        joinType,
        broadcastContext.isBhj(),
        broadcastContext.hasMixedFiltCondition(),
        broadcastContext.isExistenceJoin(),
        SubstraitUtil.toNameStruct(output).toByteArray(),
        broadcastContext.isNullAwareAntiJoin(),
        hasNullKeyValues);
  }
}
