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
package org.apache.spark.sql.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.events.GlutenPlanFallbackEvent
import org.apache.gluten.execution.VeloxWholeStageTransformerSuite

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

import scala.collection.mutable.ArrayBuffer

class VeloxExpandSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(GlutenConfig.GLUTEN_UI_ENABLED.key, "true")
      // The gluten ui event test suite expects the spark ui to be enable
      .set(UI_ENABLED, true)
  }

  test("Expand with duplicated group keys") {
    withTable("t1") {
      val events = new ArrayBuffer[GlutenPlanFallbackEvent]
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: GlutenPlanFallbackEvent => events.append(e)
            case _ =>
          }
        }
      }
      spark.sparkContext.addSparkListener(listener)

      spark.sql("""
                  |create table t1 (
                  |id int,
                  |status int,
                  |a int,
                  |b int,
                  |dt string)
                  |using parquet
                  |""".stripMargin)
      try {
        val df = spark.sql("""
                             |select dt as d1, id, dt as d2,
                             |count(distinct case when status = 1 then a end) as a_cnt,
                             |count(distinct case when status = 1 then b end) as b_cnt
                             |from t1
                             |group by dt, id, dt
                             |""".stripMargin)
        df.collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(
          !events.exists(
            _.fallbackNodeToReason.values.toSet.exists(_.contains("Failed to bind reference for"))))
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }
}
