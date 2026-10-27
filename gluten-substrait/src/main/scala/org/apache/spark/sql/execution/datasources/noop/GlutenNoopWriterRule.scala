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
package org.apache.spark.sql.execution.datasources.noop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules.injectFakeRowAdaptor
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}

/**
 * A rule that injects a FakeRowAdaptor for NoopWrite.
 *
 * The current V2 Command does not support columnar. Therefore, when its child node is a
 * ColumnarNode, Vanilla Spark inserts a ColumnarToRow conversion between V2 Command and its child.
 * This rule replaces the inserted ColumnarToRow with a FakeRowAdaptor, effectively bypassing the
 * ColumnarToRow operation for NoopWrite. Since NoopWrite does not actually perform any data
 * operations, it can accept input data in either row-based or columnar format.
 */
case class GlutenNoopWriterRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(p: SparkPlan): SparkPlan = p match {
    // Matched by field rather than positionally: Spark 4.2 added `tableName`/`transaction`
    // parameters to these node types, which breaks positional extraction.
    case rc: AppendDataExec if rc.write == NoopWrite =>
      injectFakeRowAdaptor(rc, rc.child)
    case rc: OverwriteByExpressionExec if rc.write == NoopWrite =>
      injectFakeRowAdaptor(rc, rc.child)
    case _ => p
  }
}
