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
package org.apache.spark.sql.execution.utils

import org.apache.gluten.expression.ExpressionUtils
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources

object PushDownUtil {

  def isSupportPushDownFilter(
      conf: SQLConf,
      output: Seq[Attribute],
      filter: Expression
  ): Boolean = {
    val schema = new SparkToParquetSchemaConverter(conf).convert(
      ExpressionUtils.structFromAttributes(output))
    val parquetFilters = SparkShimLoader.getSparkShims.createParquetFilters(conf, schema)
    DataSourceStrategy.translateFilter(filter, supportNestedPredicatePushdown = true) match {
      case Some(sources.StringStartsWith(_, _)) => false
      case Some(sources.Not(sources.In(_, _) | sources.StringStartsWith(_, _))) => false
      case Some(sourceFilter) =>
        parquetFilters.createFilter(sourceFilter) match {
          case Some(_) => true
          case None => false
        }
      case _ => false
    }
  }
}
