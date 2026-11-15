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
package org.apache.gluten.component

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.LanceArrowBatchScan
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionFunc}
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.util.SparkReflectionUtil

/**
 * Offloads Lance scans to Velox. A Lance scan emits Arrow batches ([[LanceArrowBatchScan]]), which
 * this component declares as Java Arrow batches, so Gluten converts them into Velox batches with
 * its Arrow transitions. Inert unless lance-spark is on the runtime classpath, so the default Velox
 * bundle carries this component without requiring Lance for other workloads.
 */
class VeloxLanceComponent extends Component {
  override def name(): String = "velox-lance"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def isRuntimeCompatible: Boolean =
    SparkReflectionUtil.isClassPresent("org.lance.spark.read.LanceScan")

  override def injectRules(injector: Injector): Unit = {
    injector.gluten.legacy.injectPreTransform(c => LanceArrowBatchScan(new GlutenConfig(c.sqlConf)))
  }

  override def convFuncOverride(): ConventionFunc.Override = new ConventionFunc.Override {
    override def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
      case scan: BatchScanExec if LanceArrowBatchScan.isArrowBatchScan(scan) => ArrowJavaBatchType
    }
  }
}
