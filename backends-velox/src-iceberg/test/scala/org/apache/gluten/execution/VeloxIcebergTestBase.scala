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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}

import scala.reflect.ClassTag

abstract class VeloxIcebergTestBase extends IcebergTestBase {

  protected def commandPhysicalPlan(df: DataFrame): SparkPlan = {
    df.queryExecution.executedPlan
      .asInstanceOf[CommandResultExec]
      .commandPhysicalPlan
  }

  protected def checkCommandPlan[T <: SparkPlan: ClassTag](df: DataFrame): Unit = {
    val plan = commandPhysicalPlan(df)
    assert(
      implicitly[ClassTag[T]].runtimeClass.isInstance(plan),
      s"Expected ${implicitly[ClassTag[T]].runtimeClass.getSimpleName}, but found:\n$plan")
  }
}
