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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, PartitioningCollection}

import scala.collection.mutable

// https://issues.apache.org/jira/browse/SPARK-31869
// Mirrors Spark 4.3 `BroadcastHashJoinExec.expandOutputPartitioning`, which also expands a
// `KeyedPartitioning` of the streamed side over the build keys.
class ExpandOutputPartitioningShim(
    streamedKeyExprs: Seq[Expression],
    buildKeyExprs: Seq[Expression],
    expandLimit: Int) {
  // An one-to-many mapping from a streamed key to build keys.
  private lazy val streamedKeyToBuildKeyMapping = {
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    streamedKeyExprs.zip(buildKeyExprs).foreach {
      case (streamedKey, buildKey) =>
        val key = streamedKey.canonicalized
        mapping.get(key) match {
          case Some(v) => mapping.put(key, v :+ buildKey)
          case None => mapping.put(key, Seq(buildKey))
        }
    }
    mapping.toMap
  }

  def expandPartitioning(partitioning: Partitioning): Partitioning = {
    expandOutputPartitioning(partitioning) match {
      case p :: Nil => p
      case ps => PartitioningCollection.fromPartitionings(ps)
    }
  }

  // Expands the given partitioning recursively. For example, if the expressions for the given
  // partitioning are Seq("a", "b", "c") where the streamed keys are Seq("b", "c") and the build
  // keys are Seq("x", "y"), the expanded partitionings have the following expressions:
  // Seq("a", "b", "c"), Seq("a", "b", "y"), Seq("a", "x", "c"), Seq("a", "x", "y").
  private def expandOutputPartitioning(partitioning: Partitioning): List[Partitioning] = {
    partitioning match {
      case c: PartitioningCollection => c.partitionings.flatMap(expandOutputPartitioning).toList
      case p: Partitioning with Expression =>
        p.multiTransformDown {
          case e: Expression if streamedKeyToBuildKeyMapping.contains(e.canonicalized) =>
            e +: streamedKeyToBuildKeyMapping(e.canonicalized)
        }.asInstanceOf[LazyList[Partitioning]]
          .take(expandLimit)
          .toList
      case other => List(other)
    }
  }
}
