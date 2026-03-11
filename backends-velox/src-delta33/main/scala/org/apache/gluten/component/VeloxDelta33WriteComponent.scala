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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.{LeafV2CommandExec, OffloadDeltaCommand}

class VeloxDelta33WriteComponent extends Component {
  override def name(): String = "velox-delta33-write"

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxDeltaComponent] :: Nil

  override def injectRules(injector: Injector): Unit = {
    val legacy = injector.gluten.legacy
    val ras = injector.gluten.ras
    legacy.injectTransform {
      c =>
        val offload = Seq(
          OffloadDeltaCommand()
        ).map(_.toStrcitRule())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload)
    }
    val offloads: Seq[RasOffload] = Seq(
      RasOffload.from[ExecutedCommandExec](OffloadDeltaCommand()),
      RasOffload.from[LeafV2CommandExec](OffloadDeltaCommand())
    )
    offloads.foreach(
      offload =>
        ras.injectRasRule(
          c => RasOffload.Rule(offload, Validators.newValidator(new GlutenConfig(c.sqlConf)), Nil)))
  }
}
