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

import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.config.{GlutenConfig, VeloxDeltaConfig}
import org.apache.gluten.extension.{DeltaCDFScanRule, DeltaPostTransformRules, OffloadDeltaFilter, OffloadDeltaProject, OffloadDeltaScan}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.util.SparkReflectionUtil

class VeloxDeltaComponent extends Component {
  override def name(): String = "velox-delta"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def isRuntimeCompatible: Boolean = {
    SparkReflectionUtil.isClassPresent("io.delta.sql.DeltaSparkSessionExtension")
  }

  override def injectRules(injector: Injector): Unit = {
    // Expands Delta CDF relations while the plan is still logical, so the Delta file scans they
    // read reach the offload rules below and Spark's optimizer handles their predicate pushdown.
    // Must not run earlier than the optimizer: analysis is eager at Dataset creation, and an
    // open-ended CDF range has to resolve to the latest version at execution time.
    injector.spark.injectOptimizerRule(
      spark =>
        DeltaCDFScanRule(
          spark,
          () => new VeloxDeltaConfig(spark.sessionState.conf).enableChangeDataFeedScan))

    val legacy = injector.gluten.legacy
    // Deletion-vector scans need no Gluten-side logical preprocessing: Delta's own
    // PreprocessTableWithDVsStrategy injects the skip-row column and filter during physical
    // planning, DeltaPostTransformRules.nativeDeletionVectorRule strips them when the scan
    // offloads, and DeltaScanTransformer materializes the per-file DV payloads for Velox.
    legacy.injectTransform {
      c =>
        val offload = Seq(OffloadDeltaScan(), OffloadDeltaProject(), OffloadDeltaFilter())
          .map(_.toStrcitRule())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload)
    }
    DeltaPostTransformRules.rules.foreach(r => legacy.injectPostTransform(_ => r))
  }
}
