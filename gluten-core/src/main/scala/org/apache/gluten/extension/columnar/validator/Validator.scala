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
package org.apache.gluten.extension.columnar.validator

import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait Validator {
  import Validator._
  def validate(plan: SparkPlan): OutCome

  final def pass(): OutCome = {
    Passed
  }

  final def fail(p: SparkPlan): OutCome = {
    Validator.Failed(s"[${getClass.getSimpleName}] Validation failed on node ${p.nodeName}")
  }

  final def fail(reason: String): OutCome = {
    Validator.Failed(reason)
  }
}

object Validator {
  sealed trait OutCome
  case object Passed extends OutCome
  case class Failed private (reason: String) extends OutCome

  private object NoopValidator extends Validator {
    override def validate(plan: SparkPlan): Validator.OutCome = pass()
  }

  def noop(): Validator = NoopValidator
  def builder(): Builder = Builder()

  class Builder private {
    import Builder._
    private val buffer: ListBuffer[Validator] = mutable.ListBuffer()

    /** Add a custom validator to pipeline. */
    def add(validator: Validator): Builder = {
      buffer ++= flatten(validator)
      this
    }

    def build(): Validator = {
      if (buffer.isEmpty) {
        return NoopValidator
      }
      if (buffer.size == 1) {
        return buffer.head
      }
      new ValidatorPipeline(buffer.toSeq)
    }

    private def flatten(validator: Validator): Seq[Validator] = validator match {
      case p: ValidatorPipeline =>
        p.validators.flatMap(flatten)
      case other => Seq(other)
    }
  }

  private object Builder {
    def apply(): Builder = new Builder()

    private class ValidatorPipeline(val validators: Seq[Validator]) extends Validator {
      assert(!validators.exists(_.isInstanceOf[ValidatorPipeline]))

      override def validate(plan: SparkPlan): Validator.OutCome = {
        val init: Validator.OutCome = pass()
        val finalOut = validators.foldLeft(init) {
          case (out, validator) =>
            out match {
              case Validator.Passed => validator.validate(plan)
              case Validator.Failed(_) => out
            }
        }
        finalOut
      }
    }
  }

  implicit class ValidatorImplicits(v: Validator) {
    def andThen(other: Validator): Validator = {
      builder().add(v).add(other).build()
    }
  }
}
