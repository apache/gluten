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

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.expression.ConverterUtils;
import org.apache.gluten.substrait.SubstraitContext;
import org.apache.gluten.substrait.expression.AggregateFunctionNode;
import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.expression.WindowFunctionNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.extensions.ExtensionBuilder;
import org.apache.gluten.substrait.type.ColumnTypeNode;
import org.apache.gluten.substrait.type.TypeBuilder;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.*;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.List;
import java.util.stream.Collectors;

/** Contains helper functions for constructing substrait relations. */
public class RelBuilder {
  private RelBuilder() {}

  public static AdvancedExtensionNode createExtensionNode(List<Attribute> inputAttributes) {
    // Use an extension node to send the input types through Substrait plan for validation.
    List<TypeNode> inputTypeNodeList =
        inputAttributes.stream()
            .map(attr -> ConverterUtils.getTypeNode(attr.dataType(), attr.nullable()))
            .collect(Collectors.toList());

    return ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance()
            .packPBMessage(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf()));
  }

  public static RelNode makeFilterRel(
      SubstraitContext context,
      ExpressionNode condExprNode,
      List<Attribute> inputAttributes,
      Long operatorId,
      RelNode input,
      Boolean validation) {
    if (!validation) {
      return RelBuilder.makeFilterRel(input, condExprNode, context, operatorId);
    } else {
      return RelBuilder.makeFilterRel(
          input, condExprNode, createExtensionNode(inputAttributes), context, operatorId);
    }
  }

  public static RelNode makeFilterRel(
      RelNode input, ExpressionNode condition, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FilterRelNode(input, condition);
  }

  public static RelNode makeFilterRel(
      RelNode input,
      ExpressionNode condition,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FilterRelNode(input, condition, extensionNode);
  }

  public static RelNode makeProjectRel(
      List<Attribute> inputAttributes,
      RelNode input,
      List<ExpressionNode> projExprNodeList,
      SubstraitContext context,
      Long operatorId,
      Boolean validation) {
    int emitStartIndex = inputAttributes.size();
    if (!validation) {
      return RelBuilder.makeProjectRel(
          input, projExprNodeList, context, operatorId, emitStartIndex);
    } else {
      return RelBuilder.makeProjectRel(
          input,
          projExprNodeList,
          createExtensionNode(inputAttributes),
          context,
          operatorId,
          emitStartIndex);
    }
  }

  public static RelNode makeProjectRel(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, -1);
  }

  public static RelNode makeProjectRel(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      SubstraitContext context,
      Long operatorId,
      int emitStartIndex) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, emitStartIndex);
  }

  public static RelNode makeProjectRel(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId,
      int emitStartIndex) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, extensionNode, emitStartIndex);
  }

  public static RelNode makeAggregateRel(
      RelNode input,
      List<ExpressionNode> groupings,
      List<AggregateFunctionNode> aggregateFunctionNodes,
      List<ExpressionNode> filters,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes, filters, extensionNode);
  }

  public static RelNode makeReadRel(
      List<TypeNode> types,
      List<String> names,
      ExpressionNode filter,
      List<ColumnTypeNode> columnTypeNodes,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, filter, columnTypeNodes, extensionNode);
  }

  public static RelNode makeReadRelForInputIterator(
      List<Attribute> attributes, SubstraitContext context, Long operatorId) {
    List<TypeNode> typeList = ConverterUtils.collectAttributeTypeNodes(attributes);
    List<String> nameList = ConverterUtils.collectAttributeNamesWithExprId(attributes);
    return makeReadRelForInputIterator(typeList, nameList, context, operatorId);
  }

  public static RelNode makeReadRelForInputIterator(
      List<TypeNode> typeList, List<String> nameList, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    Long iteratorIndex = context.nextIteratorIndex();
    return new InputIteratorRelNode(typeList, nameList, iteratorIndex);
  }

  // only used in CHHashAggregateExecTransformer for CH backend
  public static RelNode makeReadRelForInputIteratorWithoutRegister(
      List<TypeNode> typeList, List<String> nameList, SubstraitContext context) {
    Long iteratorIndex = context.currentIteratorIndex();
    return new InputIteratorRelNode(typeList, nameList, iteratorIndex);
  }

  public static RelNode makeJoinRel(
      RelNode left,
      RelNode right,
      JoinRel.JoinType joinType,
      ExpressionNode expression,
      ExpressionNode postJoinFilter,
      String hashTableId,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return makeJoinRel(
        left, right, joinType, expression, postJoinFilter, null, hashTableId, context, operatorId);
  }

  public static RelNode makeJoinRel(
      RelNode left,
      RelNode right,
      JoinRel.JoinType joinType,
      ExpressionNode expression,
      ExpressionNode postJoinFilter,
      AdvancedExtensionNode extensionNode,
      String hashTableId,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new JoinRelNode(
        left, right, joinType, expression, postJoinFilter, hashTableId, extensionNode);
  }

  public static RelNode makeCrossRel(
      RelNode left,
      RelNode right,
      CrossRel.JoinType joinType,
      ExpressionNode expression,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new CrossRelNode(left, right, joinType, expression, extensionNode);
  }

  public static RelNode makeExpandRel(
      RelNode input,
      List<List<ExpressionNode>> projections,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ExpandRelNode(input, projections, extensionNode);
  }

  public static RelNode makeExpandRel(
      RelNode input,
      List<List<ExpressionNode>> projections,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ExpandRelNode(input, projections);
  }

  public static RelNode makeWriteRel(
      RelNode input,
      List<TypeNode> types,
      List<String> names,
      List<ColumnTypeNode> columnTypeNodes,
      AdvancedExtensionNode extensionNode,
      WriteRel.BucketSpec bucketSpec,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WriteRelNode(input, types, names, columnTypeNodes, extensionNode, bucketSpec);
  }

  public static RelNode makeSortRel(
      RelNode input,
      List<SortField> sorts,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SortRelNode(input, sorts, extensionNode);
  }

  public static RelNode makeSortRel(
      RelNode input, List<SortField> sorts, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SortRelNode(input, sorts);
  }

  public static RelNode makeFetchRel(
      RelNode input, Long offset, Long count, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FetchRelNode(input, offset, count);
  }

  public static RelNode makeFetchRel(
      RelNode input,
      Long offset,
      Long count,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FetchRelNode(input, offset, count, extensionNode);
  }

  public static RelNode makeTopNRel(
      RelNode input, Long n, List<SortField> sorts, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new TopNNode(input, n, sorts);
  }

  public static RelNode makeTopNRel(
      RelNode input,
      Long n,
      List<SortField> sorts,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new TopNNode(input, n, sorts, extensionNode);
  }

  public static RelNode makeWindowRel(
      RelNode input,
      List<WindowFunctionNode> windowFunctionNodes,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WindowRelNode(
        input, windowFunctionNodes, partitionExpressions, sorts, extensionNode);
  }

  public static RelNode makeWindowRel(
      RelNode input,
      List<WindowFunctionNode> windowFunctionNodes,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WindowRelNode(
        input, windowFunctionNodes,
        partitionExpressions, sorts);
  }

  public static RelNode makeWindowGroupLimitRel(
      RelNode input,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      Integer limit,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WindowGroupLimitRelNode(input, partitionExpressions, sorts, limit, extensionNode);
  }

  public static RelNode makeWindowGroupLimitRel(
      RelNode input,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      Integer limit,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WindowGroupLimitRelNode(input, partitionExpressions, sorts, limit);
  }

  public static RelNode makeGenerateRel(
      RelNode input,
      ExpressionNode generator,
      List<ExpressionNode> childOutput,
      boolean outer,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new GenerateRelNode(input, generator, childOutput, outer);
  }

  public static RelNode makeGenerateRel(
      RelNode input,
      ExpressionNode generator,
      List<ExpressionNode> childOutput,
      AdvancedExtensionNode extensionNode,
      boolean outer,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new GenerateRelNode(input, generator, childOutput, extensionNode, outer);
  }

  public static RelNode makeSetRel(
      List<RelNode> inputs, SetRel.SetOp setOp, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SetRelNode(inputs, setOp);
  }

  public static RelNode makeSetRel(
      List<RelNode> inputs,
      SetRel.SetOp setOp,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SetRelNode(inputs, setOp, extensionNode);
  }
}
