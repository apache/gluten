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
package org.apache.spark.sql.gluten

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import scala.reflect.ClassTag

object TestUtils {

  def checkExecutedPlanContains[T: ClassTag](spark: SparkSession, sqlStr: String): Unit = {
    var found = false
    val queryListener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        if (!found) {
          found = qe.executedPlan.find(implicitly[ClassTag[T]].runtimeClass.isInstance(_)).isDefined
        }
      }
    }
    try {
      spark.listenerManager.register(queryListener)
      spark.sql(sqlStr)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(found)
    } finally {
      spark.listenerManager.unregister(queryListener)
    }
  }
}
