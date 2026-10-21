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
package org.apache.gluten.extension

import org.apache.gluten.config.{GlutenConfig, VeloxLanceConfig}
import org.apache.gluten.execution.{LanceWriteExec, VeloxLanceAppendDataExec, VeloxLanceOverwriteByExpressionExec}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}

/** Replaces a vanilla append into a Lance table with [[VeloxLanceAppendDataExec]]. */
case class OffloadLanceAppend() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (!VeloxLanceConfig.get.enableNativeWrite) {
      return plan
    }
    plan match {
      case a: AppendDataExec if LanceWriteExec.supportsWrite(a.write) =>
        VeloxLanceAppendDataExec(a)
      case other => other
    }
  }
}

/**
 * Replaces a vanilla overwrite of a Lance table with [[VeloxLanceOverwriteByExpressionExec]]. Lance
 * only supports truncating overwrites, so this replaces the whole table, as for `mode("overwrite")`
 * or `INSERT OVERWRITE`.
 */
case class OffloadLanceOverwrite() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = {
    if (!VeloxLanceConfig.get.enableNativeWrite) {
      return plan
    }
    plan match {
      case o: OverwriteByExpressionExec if LanceWriteExec.supportsWrite(o.write) =>
        VeloxLanceOverwriteByExpressionExec(o)
      case other => other
    }
  }
}

object OffloadLanceWrite {
  def inject(injector: Injector): Unit = {
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadLanceAppend(), OffloadLanceOverwrite())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }
  }
}
