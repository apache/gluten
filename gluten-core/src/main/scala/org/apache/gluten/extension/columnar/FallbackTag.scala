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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.{TreeNode, TreeNodeTag}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.commons.lang3.exception.ExceptionUtils

sealed trait FallbackTag {
  val stacktrace: Option[String] =
    if (FallbackTags.DEBUG) {
      Some(ExceptionUtils.getStackTrace(new Throwable()))
    } else None

  def reason(): String
}

object FallbackTag {

  /** A tag that stores one reason text of fall back. */
  case class Appendable(override val reason: String) extends FallbackTag

  /**
   * A tag that stores reason text of fall back. Other reasons will be discarded when this tag is
   * added to plan.
   */
  case class Exclusive(override val reason: String) extends FallbackTag

  trait Converter[T] {
    def from(obj: T): Option[FallbackTag]
  }

  object Converter {
    implicit def asIs[T <: FallbackTag]: Converter[T] = (tag: T) => Some(tag)

    implicit object FromString extends Converter[String] {
      override def from(reason: String): Option[FallbackTag] = Some(Appendable(reason))
    }
  }
}

object FallbackTags {
  val TAG: TreeNodeTag[FallbackTag] =
    TreeNodeTag[FallbackTag]("org.apache.gluten.FallbackTag")

  val DEBUG = false

  /**
   * If true, the plan node will be guaranteed fallback to Vanilla plan node while being
   * implemented.
   *
   * If false, the plan still has chance to be turned into "non-transformable" in any another
   * validation rule. So user should not consider the plan "transformable" unless all validation
   * rules are passed.
   */
  def nonEmpty(plan: SparkPlan): Boolean = {
    getOption(plan).nonEmpty
  }

  /**
   * If true, it implies the plan maybe transformable during validation phase but not guaranteed,
   * since another validation rule could turn it to "non-transformable" before implementing the plan
   * within Gluten transformers. If false, the plan node will be guaranteed fallback to Vanilla plan
   * node while being implemented.
   */
  def maybeOffloadable(plan: SparkPlan): Boolean = !nonEmpty(plan)

  def add[T](plan: TreeNode[_], t: T)(implicit converter: FallbackTag.Converter[T]): Unit = {
    val tagOption = getOption(plan)
    val newTagOption = converter.from(t)

    val mergedTagOption: Option[FallbackTag] =
      (tagOption ++ newTagOption).reduceOption[FallbackTag] {
        // New tag comes while the plan was already tagged, merge.
        case (_, exclusive: FallbackTag.Exclusive) =>
          exclusive
        case (exclusive: FallbackTag.Exclusive, _) =>
          exclusive
        case (l: FallbackTag.Appendable, r: FallbackTag.Appendable) =>
          FallbackTag.Appendable(s"${l.reason}; ${r.reason}")
      }
    mergedTagOption
      .foreach(mergedTag => plan.setTagValue(TAG, mergedTag))
  }

  def untag(plan: TreeNode[_]): Unit = {
    plan.unsetTagValue(TAG)
  }

  def get(plan: TreeNode[_]): FallbackTag = {
    getOption(plan).getOrElse(
      throw new IllegalStateException("Transform hint tag not set in plan: " + plan.nodeName))
  }

  def getOption(plan: TreeNode[_]): Option[FallbackTag] = {
    plan.getTagValue(TAG)
  }
}

case class RemoveFallbackTagRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach(FallbackTags.untag)
    plan
  }
}
