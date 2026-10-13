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
package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.aggregate.ScalaUDAF

object HiveUDAFInspector {
  def getUDAFClassName(expr: Expression): Option[String] = {
    expr match {
      case func: HiveUDAFFunction => Some(func.funcWrapper.functionClassName)
      case scalaUDAF: ScalaUDAF => Some(scalaUDAF.udaf.getClass.getName)
      case _ => None
    }
  }

  /**
   * Whether this is an old-style Hive UDAF, which Spark runs by wrapping in a GenericUDAFBridge.
   * The bridge's getEvaluator resolves over the implementation's iterate methods, so unlike an
   * AbstractGenericUDAFResolver -- which receives the actual ObjectInspectors and either accepts
   * them or throws -- Hive picks the implementation by resolving an overload.
   *
   * Exposed here because HiveUDAFFunction is private to this package.
   */
  def isBridgedLegacyUDAF(expr: Expression): Boolean = expr match {
    case func: HiveUDAFFunction => func.isUDAFBridgeRequired
    case _ => false
  }
}
