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
package org.apache.gluten.extension.injector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.{SparkStrategy => Strategy}

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method}

import scala.collection.mutable

class InjectorControl private[injector] () {
  import InjectorControl._
  private val disablerBuffer: mutable.ListBuffer[Disabler] =
    mutable.ListBuffer()
  private var combined: Disabler = (_: SparkSession) => false

  def disableOn(one: Disabler): Unit = synchronized {
    disablerBuffer += one
    // Update the combined disabler.
    val disablerList = disablerBuffer.toList
    combined = s => disablerList.exists(_.disabled(s))
  }

  private[injector] def disabler(): Disabler = synchronized {
    combined
  }
}

object InjectorControl {
  trait Disabler {
    // If true, the injected rule will be disabled.
    protected[injector] def disabled(session: SparkSession): Boolean
  }

  private object Disabler {
    implicit private[injector] class DisablerOps(disabler: Disabler) {
      def wrapRule[TreeType <: TreeNode[_]](
          ruleBuilder: SparkSession => Rule[TreeType]): SparkSession => Rule[TreeType] =
        session => {
          val rule = ruleBuilder(session)
          new Rule[TreeType] with DisablerAware {
            override val ruleName: String = rule.ruleName
            override def apply(plan: TreeType): TreeType = {
              if (disabler.disabled(session)) {
                return plan
              }
              rule(plan)
            }
          }
        }

      def wrapStrategy(strategyBuilder: StrategyBuilder): StrategyBuilder = session => {
        val strategy = strategyBuilder(session)
        new Strategy with DisablerAware {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
            if (disabler.disabled(session)) {
              return Nil
            }
            strategy(plan)
          }
        }
      }

      def wrapParser(parserBuilder: ParserBuilder): ParserBuilder = (session, parser) => {
        val before = parser
        val after = parserBuilder(session, before)
        // Use dynamic proxy to get rid of 3.2 compatibility issues.
        java.lang.reflect.Proxy
          .newProxyInstance(
            classOf[ParserInterface].getClassLoader,
            Array(classOf[ParserInterface], classOf[DisablerAware]),
            new InvocationHandler {
              override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
                try {
                  if (disabler.disabled(session)) {
                    return method.invoke(before, args: _*)
                  }
                  method.invoke(after, args: _*)
                } catch {
                  case e: InvocationTargetException =>
                    // Unwrap the ITE.
                    throw e.getCause
                }
              }
            }
          )
          .asInstanceOf[ParserInterface]
      }

      def wrapFunction(functionDescription: FunctionDescription): FunctionDescription = {
        val (identifier, info, builder) = functionDescription
        val wrappedBuilder: FunctionBuilder = new FunctionBuilder with DisablerAware {
          override def apply(children: Seq[Expression]): Expression = {
            if (
              disabler.disabled(SparkSession.getActiveSession.getOrElse(
                throw new IllegalStateException("Active Spark session not found")))
            ) {
              throw new UnsupportedOperationException(
                s"Function ${info.getName} is not callable as Gluten is disabled")
            }
            builder(children)
          }
        }
        (identifier, info, wrappedBuilder)
      }

      def wrapColumnarRule(columnarRuleBuilder: ColumnarRuleBuilder): ColumnarRuleBuilder =
        session => {
          val columnarRule = columnarRuleBuilder(session)
          new ColumnarRule with DisablerAware {
            override val preColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.preColumnarTransitions.apply(plan)
                }
              }
            }

            override val postColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.postColumnarTransitions.apply(plan)
                }
              }
            }
          }
        }
    }
  }

  /**
   * The entity (could be a rule, a parser, cost evaluator) that is dynamically injected to Spark,
   * whose effectivity is under the control by a disabler.
   */
  trait DisablerAware
}
