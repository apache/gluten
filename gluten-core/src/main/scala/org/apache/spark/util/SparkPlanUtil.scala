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
package org.apache.spark.util

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf

object SparkPlanUtil {

  def supportsRowBased(plan: SparkPlan): Boolean = {
    val m = classOf[SparkPlan].getMethod("supportsRowBased")
    m.invoke(plan).asInstanceOf[Boolean]
  }

  def isPlannedV1Write(plan: DataWritingCommandExec): Boolean = {
    if (SparkVersionUtil.eqSpark33) {
      return false
    }

    val v1WriteCommandClass =
      Utils.classForName("org.apache.spark.sql.execution.datasources.V1WriteCommand")
    val plannedWriteEnabled =
      SQLConf.get.getConfString("spark.sql.optimizer.plannedWrite.enabled", "true").toBoolean
    v1WriteCommandClass.isAssignableFrom(plan.cmd.getClass) && plannedWriteEnabled
  }
}
