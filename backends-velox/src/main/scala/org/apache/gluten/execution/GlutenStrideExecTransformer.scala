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
package org.apache.gluten.execution

import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.JavaConverters._

/**
 * Gluten-native "stride" operator: outputs every N-th row from each input batch.
 *
 * Row indices within each batch are 0-based; index 0 is always included. The counter resets per
 * batch, so the operator is completely stateless and parallelism-friendly.
 *
 * This is a self-contained example of the Gluten custom-operator mechanism. It does NOT correspond
 * to any existing Spark operator -- it exists purely to demonstrate how to add a backend-specific
 * native operator without requiring a Velox upstream contribution.
 *
 * ==End-to-end flow==
 * {{{
 *   GlutenStrideExecTransformer(stride=3, child=scanPlan)
 *     --doTransform-->  GlutenStrideRelNode(stride=3)
 *       --toProtobuf--> FetchRel { offset=3, advanced_extension.optimization="isGlutenStride=1" }
 *         --JNI-->      SubstraitToVeloxPlanConverter::toVeloxPlan(FetchRel&)
 *           --builds--> GlutenStrideNode(stride=3, child)
 *             --exec--> GlutenStrideOperator: keeps rows 0, 3, 6, 9, ...
 * }}}
 *
 * ==Usage==
 * Instantiate directly in a unit test or wire it into a custom offload rule:
 * {{{
 *   val strider = GlutenStrideExecTransformer(stride = 3L, child = childPlan)
 * }}}
 */
case class GlutenStrideExecTransformer(stride: Long, child: SparkPlan)
  extends UnaryTransformSupport {

  require(stride >= 1L, s"stride must be >= 1, got $stride")

  // -------------------------------------------------------------------------
  // SparkPlan identity
  // -------------------------------------------------------------------------

  override def output: Seq[Attribute] = child.output

  override def metricsUpdater(): MetricsUpdater = MetricsUpdater.None

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenStrideExecTransformer =
    copy(child = newChild)

  // -------------------------------------------------------------------------
  // Validation
  // -------------------------------------------------------------------------

  override protected def doValidateInternal(): ValidationResult = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = makeRelNode(context, operatorId, inputRelNode = null, validation = true)
    doNativeValidation(context, relNode)
  }

  // -------------------------------------------------------------------------
  // Transformation
  // -------------------------------------------------------------------------

  override protected def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    val relNode = makeRelNode(context, operatorId, inputRelNode = childCtx.root, validation = false)
    TransformContext(output, relNode)
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private def makeRelNode(
      context: SubstraitContext,
      operatorId: Long,
      inputRelNode: RelNode,
      validation: Boolean): RelNode = {
    if (validation) {
      RelBuilder.makeGlutenStrideRel(
        inputRelNode,
        stride,
        RelBuilder.createExtensionNode(output.asJava),
        context,
        operatorId)
    } else {
      RelBuilder.makeGlutenStrideRel(inputRelNode, stride, context, operatorId)
    }
  }
}
