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
package org.apache.spark.sql.execution.python

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedArgumentExpression}

abstract class EvalPythonExecBase extends EvalPythonExec {

  override protected def evaluatorFactory: EvalPythonEvaluatorFactory = {
    throw new IllegalStateException("EvalPythonExecTransformer doesn't support evaluate")
  }
}

object EvalPythonExecBase {
  object NamedArgumentExpressionShim {
    def unapply(expr: Expression): Option[(String, Expression)] = expr match {
      case NamedArgumentExpression(key, value) => Some((key, value))
      case _ => None
    }
  }
}
