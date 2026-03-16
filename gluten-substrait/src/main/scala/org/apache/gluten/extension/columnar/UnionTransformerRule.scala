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
package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ColumnarUnionExec, UnionExecTransformer}

import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * Replace ColumnarUnionExec with UnionExecTransformer if possible.
 *
 * The rule relies on children's output partitioning to be fully provided.
 */
case class UnionTransformerRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableNativeUnion) {
      return plan
    }
    plan.transformUp {
      case plan: ColumnarUnionExec =>
        val transformer = UnionExecTransformer(plan.children)
        if (sameNumPartitions(plan.children) && validate(transformer)) {
          transformer
        } else {
          plan
        }
    }
  }

  private def sameNumPartitions(plans: Seq[SparkPlan]): Boolean = {
    val partitioning = plans.map(_.outputPartitioning)
    if (partitioning.exists(p => p.isInstanceOf[UnknownPartitioning])) {
      return false
    }
    val numPartitions = plans.map(_.outputPartitioning.numPartitions)
    numPartitions.forall(_ == numPartitions.head)
  }

  private def validate(union: UnionExecTransformer): Boolean = {
    union.doValidate().ok()
  }
}
