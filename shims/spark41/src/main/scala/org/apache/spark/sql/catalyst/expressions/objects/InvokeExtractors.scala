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
package org.apache.spark.sql.catalyst.expressions.objects

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator

/**
 * Extractors for Invoke expressions to ensure compatibility across different Spark versions.
 *
 * Since Spark 4.0, StructsToJson has been replaced with Invoke expressions using
 * StructsToJsonEvaluator. This extractor provides a unified interface to extract evaluator options,
 * child expression, and timeZoneId from the Invoke pattern.
 */
object StructsToJsonInvoke {
  def unapply(expr: Expression): Option[(Map[String, String], Expression, Option[String])] = {
    expr match {
      case Invoke(
            Literal(evaluator: StructsToJsonEvaluator, _),
            "evaluate",
            _,
            Seq(child),
            _,
            _,
            _,
            _) =>
        Some((evaluator.options, child, evaluator.timeZoneId))
      case _ =>
        None
    }
  }
}
