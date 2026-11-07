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
package org.apache.gluten.utils.bolt

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}

import org.apache.spark.GlutenSortShuffleSuite
import org.apache.spark.sql.GlutenBoltBackendSmokeSuite
import org.apache.spark.sql.catalyst.expressions.{GlutenArithmeticExpressionSuite, GlutenBitwiseExpressionsSuite, GlutenConditionalExpressionSuite, GlutenLiteralExpressionSuite, GlutenNullExpressionsSuite, GlutenPredicateSuite}
import org.apache.spark.sql.hive.execution.GlutenHiveExplainSuite

import java.nio.file.Paths

class BoltTestSettings extends BackendTestSettings {
  enableSuite[GlutenSortShuffleSuite]
  enableSuite[GlutenBoltBackendSmokeSuite]
  enableSuite[GlutenArithmeticExpressionSuite]
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenLiteralExpressionSuite]
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
  enableSuite[GlutenHiveExplainSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = BoltSQLQueryTestSettings
}

private object BoltSQLQueryTestSettings extends SQLQueryTestSettings {
  // Use the input and expected output shipped together in this Spark release.
  override def getResourceFilePath: String = {
    val home = sys.props.get("spark.test.home").orElse(sys.env.get("SPARK_HOME")).getOrElse(".")
    Paths.get(home, "sql/core/src/test/resources/sql-tests").toString
  }
  override def getSupportedSQLQueryTests: Set[String] =
    Set("bitwise.sql", "conditional-functions.sql", "count.sql", "null-handling.sql")
  override def getOverwriteSQLQueryTests: Set[String] = Set.empty
}
