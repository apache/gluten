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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}

import scala.util.control.Breaks.{break, breakable}

// BroadcastHashJoinExec and it's child BroadcastExec will be cut into different QueryStages,
// so the columnar rules will be applied to the two QueryStages separately, and they cannot
// see each other during transformation. In order to prevent BroadcastExec being transformed
// to columnar while BHJ fallbacks, BroadcastExec need to be tagged not transformable when applying
// queryStagePrepRules.
case class FallbackBroadcastHashJoinPrepQueryStage(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val glutenConf: GlutenConfig = GlutenConfig.get
    plan.foreach {
      case bhj: BroadcastHashJoinExec =>
        val buildSidePlan = bhj.buildSide match {
          case BuildLeft => bhj.left
          case BuildRight => bhj.right
        }
        val maybeExchange = buildSidePlan.find {
          case BroadcastExchangeExec(_, _) => true
          case _ => false
        }
        maybeExchange match {
          case Some(exchange @ BroadcastExchangeExec(mode, child)) =>
            val isTransformable =
              if (
                !glutenConf.enableColumnarBroadcastExchange ||
                !glutenConf.enableColumnarBroadcastJoin
              ) {
                ValidationResult.failed(
                  "columnar broadcast exchange is disabled or " +
                    "columnar broadcast join is disabled")
              } else {
                if (FallbackTags.nonEmpty(bhj)) {
                  ValidationResult.failed("broadcast join is already tagged as not transformable")
                } else {
                  val bhjTransformer = BackendsApiManager.getSparkPlanExecApiInstance
                    .genBroadcastHashJoinExecTransformer(
                      bhj.leftKeys,
                      bhj.rightKeys,
                      bhj.joinType,
                      bhj.buildSide,
                      bhj.condition,
                      bhj.left,
                      bhj.right,
                      bhj.isNullAwareAntiJoin)
                  val isBhjTransformable = bhjTransformer.doValidate()
                  if (isBhjTransformable.ok()) {
                    val exchangeTransformer = ColumnarBroadcastExchangeExec(mode, child)
                    exchangeTransformer.doValidate()
                  } else {
                    isBhjTransformable
                  }
                }
              }
            FallbackTags.add(bhj, isTransformable)
            FallbackTags.add(exchange, isTransformable)
          case _ =>
          // Skip. This might be the case that the exchange was already
          // executed in earlier stage
        }
      case bnlj: BroadcastNestedLoopJoinExec => applyBNLJPrepQueryStage(bnlj)
      case _ =>
    }
    plan
  }

  private def applyBNLJPrepQueryStage(bnlj: BroadcastNestedLoopJoinExec) = {
    val buildSidePlan = bnlj.buildSide match {
      case BuildLeft => bnlj.left
      case BuildRight => bnlj.right
    }
    val maybeExchange = buildSidePlan.find {
      case BroadcastExchangeExec(_, _) => true
      case _ => false
    }
    maybeExchange match {
      case Some(exchange @ BroadcastExchangeExec(mode, child)) =>
        val isTransformable =
          if (
            !GlutenConfig.get.enableColumnarBroadcastExchange ||
            !GlutenConfig.get.enableColumnarBroadcastJoin
          ) {
            ValidationResult.failed(
              "columnar broadcast exchange is disabled or " +
                "columnar broadcast join is disabled")
          } else {
            if (FallbackTags.nonEmpty(bnlj)) {
              ValidationResult.failed("broadcast join is already tagged as not transformable")
            } else {
              val bnljTransformer = BackendsApiManager.getSparkPlanExecApiInstance
                .genBroadcastNestedLoopJoinExecTransformer(
                  bnlj.left,
                  bnlj.right,
                  bnlj.buildSide,
                  bnlj.joinType,
                  bnlj.condition)
              val isBnljTransformable = bnljTransformer.doValidate()
              if (isBnljTransformable.ok()) {
                val exchangeTransformer = ColumnarBroadcastExchangeExec(mode, child)
                exchangeTransformer.doValidate()
              } else {
                isBnljTransformable
              }
            }
          }
        FallbackTags.add(bnlj, isTransformable)
        FallbackTags.add(exchange, isTransformable)
      case _ =>
      // Skip. This might be the case that the exchange was already
      // executed in earlier stage
    }
  }
}

// For similar purpose with FallbackBroadcastHashJoinPrepQueryStage, executed during applying
// columnar rules.
case class FallbackBroadcastHashJoin(session: SparkSession) extends Rule[SparkPlan] {

  private val enableColumnarBroadcastJoin: Boolean =
    GlutenConfig.get.enableColumnarBroadcastJoin &&
      GlutenConfig.get.enableColumnarBroadcastExchange

  private val enableColumnarBroadcastNestedLoopJoin: Boolean =
    GlutenConfig.get.enableColumnarBroadcastNestedLoopJoin &&
      GlutenConfig.get.enableColumnarBroadcastExchange

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp {
      p =>
        try {
          p match {
            case bhj: BroadcastHashJoinExec =>
              // FIXME Hongze: In following codes we perform a lot of if-else conditions to
              //  make sure the broadcast exchange and broadcast hash-join are of same type,
              //  either vanilla or columnar. In order to simplify the codes we have to do
              //  some tricks around C2R and R2C to make them adapt to columnar broadcast.
              //  Currently their doBroadcast() methods just propagate child's broadcast
              //  payloads which is not right in speaking of columnar.
              if (!enableColumnarBroadcastJoin) {
                FallbackTags.add(
                  bhj,
                  "columnar BroadcastJoin is not enabled in BroadcastHashJoinExec")
              } else {
                val isBhjTransformable: ValidationResult = {
                  val transformer = BackendsApiManager.getSparkPlanExecApiInstance
                    .genBroadcastHashJoinExecTransformer(
                      bhj.leftKeys,
                      bhj.rightKeys,
                      bhj.joinType,
                      bhj.buildSide,
                      bhj.condition,
                      bhj.left,
                      bhj.right,
                      isNullAwareAntiJoin = bhj.isNullAwareAntiJoin)
                  transformer.doValidate()
                }
                val buildSidePlan = bhj.buildSide match {
                  case BuildLeft => bhj.left
                  case BuildRight => bhj.right
                }

                preTagBroadcastExchangeFallback(bhj, buildSidePlan, isBhjTransformable)
              }
            case bnlj: BroadcastNestedLoopJoinExec => applyBNLJFallback(bnlj)
            case _ =>
          }
        } catch {
          case e: UnsupportedOperationException =>
            FallbackTags.add(
              p,
              s"${e.getMessage}, original Spark plan is " +
                s"${p.getClass}(${p.children.toList.map(_.getClass)})")
        }
    }
    plan
  }

  private def applyBNLJFallback(bnlj: BroadcastNestedLoopJoinExec) = {
    if (!enableColumnarBroadcastNestedLoopJoin) {
      FallbackTags.add(bnlj, "columnar BroadcastJoin is not enabled in BroadcastNestedLoopJoinExec")
    }

    val transformer = BackendsApiManager.getSparkPlanExecApiInstance
      .genBroadcastNestedLoopJoinExecTransformer(
        bnlj.left,
        bnlj.right,
        bnlj.buildSide,
        bnlj.joinType,
        bnlj.condition)

    val isBNLJTransformable = transformer.doValidate()
    val buildSidePlan = bnlj.buildSide match {
      case BuildLeft => bnlj.left
      case BuildRight => bnlj.right
    }

    preTagBroadcastExchangeFallback(bnlj, buildSidePlan, isBNLJTransformable)
  }

  private def preTagBroadcastExchangeFallback(
      plan: SparkPlan,
      buildSidePlan: SparkPlan,
      isTransformable: ValidationResult): Unit = {
    val maybeExchange = buildSidePlan
      .find {
        case BroadcastExchangeExec(_, _) => true
        case _ => false
      }
      .map(_.asInstanceOf[BroadcastExchangeExec])

    maybeExchange match {
      case Some(exchange @ BroadcastExchangeExec(_, _)) =>
        isTransformable.tagOnFallback(plan)
        if (!isTransformable.ok) {
          FallbackTags.add(exchange, isTransformable)
        }
      case None =>
        // we are in AQE, find the hidden exchange
        // FIXME did we consider the case that AQE: OFF && Reuse: ON ?
        var maybeHiddenExchange: Option[BroadcastExchangeLike] = None
        breakable {
          buildSidePlan.foreach {
            case e: BroadcastExchangeLike =>
              maybeHiddenExchange = Some(e)
              break
            case t: BroadcastQueryStageExec =>
              t.plan.foreach {
                case e2: BroadcastExchangeLike =>
                  maybeHiddenExchange = Some(e2)
                  break
                case r: ReusedExchangeExec =>
                  r.child match {
                    case e2: BroadcastExchangeLike =>
                      maybeHiddenExchange = Some(e2)
                      break
                    case _ =>
                  }
                case _ =>
              }
            case _ =>
          }
        }
        // restriction to force the hidden exchange to be found
        val exchange = maybeHiddenExchange.get
        // to conform to the underlying exchange's type, columnar or vanilla
        exchange match {
          case BroadcastExchangeExec(mode, child) =>
            FallbackTags.add(
              plan,
              "it's a materialized broadcast exchange or reused broadcast exchange")
          case ColumnarBroadcastExchangeExec(mode, child) =>
            if (!isTransformable.ok) {
              throw new IllegalStateException(
                s"BroadcastExchange has already been" +
                  s" transformed to columnar version but BHJ is determined as" +
                  s" non-transformable: ${plan.toString()}")
            }
        }
    }
  }
}
