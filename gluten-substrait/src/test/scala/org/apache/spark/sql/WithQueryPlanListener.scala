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
package org.apache.spark.sql

import org.apache.gluten.test.FallbackUtil.collectWithSubqueries

import org.apache.spark.sql.WithQueryPlanListener.ListenerImpl
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuiteLike

import scala.collection.mutable
import scala.reflect.ClassTag
// format: off
/**
 * A trait that registers listeners to collect all the executed query plans during running a single
 * test case.
 *
 * The collected query plans can be used to verify the correctness of the query execution plan.
 */
// format: on
trait WithQueryPlanListener extends SharedSparkSession with AnyFunSuiteLike {

  assert(
    !this.isInstanceOf[SparkPlanTest],
    "WithQueryPlanListener should not be mixed in with SparkPlanTest as it doesn't intercept " +
      "the `SparkPlanTest.checkAnswer` calls"
  )

  protected val planListeners: WithQueryPlanListener.QueryPlanListeners =
    new WithQueryPlanListener.QueryPlanListeners()

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      testFun
      listeners().foreach(_.invokeAll())
    }(pos)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sessionState.listenerManager.register(new ListenerImpl(planListeners.collect()))
  }

  override def afterEach(): Unit = {
    listeners().foreach(listener => spark.sessionState.listenerManager.unregister(listener))
    super.afterEach()
  }

  private def listeners() = {
    spark.sessionState.listenerManager
      .listListeners()
      .filter(_.isInstanceOf[ListenerImpl])
      .map(_.asInstanceOf[ListenerImpl])
      .toSeq
  }
}

object WithQueryPlanListener {
  // A listener that collects all the executed query plans during running a single test case.
  private class ListenerImpl(val listeners: Seq[QueryPlanListener])
    extends QueryExecutionListener
    with AdaptiveSparkPlanHelper {
    private val plans = mutable.ArrayBuffer[SparkPlan]()

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      val plan = qe.executedPlan
      plans += plan
    }

    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

    def invokeAll(): Unit = {
      while (plans.nonEmpty) {
        val plansCopy = Seq[SparkPlan](plans.toSeq: _*)
        plans.clear()
        plansCopy.foreach(plan => listeners.foreach(listener => listener.apply(plan)))
      }
    }
  }

  class QueryPlanListeners private[WithQueryPlanListener] {
    private val builder = mutable.ArrayBuffer[QueryPlanListener]()

    private[WithQueryPlanListener] def collect(): Seq[QueryPlanListener] = {
      builder.toSeq
    }

    def onPlanExecuted(listener: QueryPlanListener): QueryPlanListeners = {
      builder += listener
      this
    }

    def assertNoNodeExists(prediction: SparkPlan => Boolean): QueryPlanListeners = {
      builder += {
        planRoot =>
          val allNodes = collectWithSubqueries(planRoot) { case p => p }
          allNodes.foreach {
            node =>
              assert(
                !prediction(node),
                s"Found a node ${node.nodeName} in the query plan that fails the assertion: " +
                  s"${planRoot.toString}")
          }
      }
      this
    }

    def assertNoInstanceOf[T: ClassTag]: QueryPlanListeners = {
      val clazz = implicitly[ClassTag[T]].runtimeClass
      assertNoNodeExists(n => clazz.isAssignableFrom(n.getClass))
    }
  }

  type QueryPlanListener = SparkPlan => Unit
}
