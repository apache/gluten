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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.ColumnarToColumnarTransition

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.debug.DebugExec

package object transition {

  type TransitionGraph = FloydWarshallGraph[TransitionGraph.Vertex, Transition]
  // These 5 plan operators (as of Spark 3.5) are operators that have the
  // same convention with their children.
  //
  // Extend this list in shim layer once Spark has more.
  def canPropagateConvention(plan: SparkPlan): Boolean = plan match {
    case p: DebugExec => true
    case p: UnionExec => true
    case p: AQEShuffleReadExec => true
    case p: InputAdapter => true
    case p: WholeStageCodegenExec => true
    case _ => false
  }

  // Extractor for Spark/Gluten's C2R
  object ColumnarToRowLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case c2r: ColumnarToRowTransition =>
          Some(c2r.child)
        case _ => None
      }
    }
  }

  // Extractor for Spark/Gluten's R2C
  object RowToColumnarLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case r2c: RowToColumnarTransition =>
          Some(r2c.child)
        case _ => None
      }
    }
  }

  // Extractor for Gluten's C2C with different convention
  object ColumnarToColumnarLike {
    def unapply(plan: SparkPlan): Option[SparkPlan] = {
      plan match {
        case c2c: ColumnarToColumnarTransition =>
          Some(c2c.child)
        case _ => None
      }
    }
  }

  case class DummySparkPlan() extends LeafExecNode {
    override def supportsColumnar: Boolean = true // To bypass the assertion in ColumnarToRowExec.
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = Nil
  }
}
