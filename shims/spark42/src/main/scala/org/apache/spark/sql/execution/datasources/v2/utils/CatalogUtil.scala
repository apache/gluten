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
package org.apache.spark.sql.execution.datasources.v2.utils

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.connector.expressions.Transform

object CatalogUtil {

  def convertPartitionTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.TransformHelper
    val (identityCols, bucketSpec, _) = partitions.convertTransforms
    (identityCols, bucketSpec)
  }
}
