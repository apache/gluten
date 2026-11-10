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

import org.apache.spark.sql.execution.UnaryExecNode

/**
 * Spark 3.5 has changed the parameter type of the generateTreeString API in TreeNode. In order to
 * support multiple versions of Spark, we cannot directly override the generateTreeString method in
 * WhostageTransformer. Therefore, we have defined the GenerateTreeStringShim trait in the shim to
 * allow different Spark versions to override their own generateTreeString.
 */

trait WholeStageTransformerGenerateTreeStringShim extends UnaryExecNode {

  def stageId: Int

  def substraitPlanJson: String

  def wholeStageTransformerContextDefined: Boolean

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      printOutputColumns: Boolean,
      indent: Int = 0): Unit = {
    val prefix = if (printNodeId) "^ " else s"^($stageId) "
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix = false,
      maxFields,
      printNodeId = printNodeId,
      printOutputColumns = printOutputColumns,
      indent = indent)

    if (verbose && wholeStageTransformerContextDefined) {
      append(prefix + "Substrait plan:\n")
      append(substraitPlanJson)
      append("\n")
    }
  }
}

trait InputAdapterGenerateTreeStringShim extends UnaryExecNode {

  override def generateTreeString(
      depth: Int,
      lastChildren: java.util.ArrayList[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      printOutputColumns: Boolean,
      indent: Int = 0): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix = "",
      addSuffix = false,
      maxFields,
      printNodeId,
      printOutputColumns,
      indent)
  }
}
