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

import org.apache.spark.sql.GlutenSQLTestsBaseTrait

import org.scalactic.source.Position
import org.scalatest.Tag

class GlutenSparkSqlParserSuite extends SparkSqlParserSuite with GlutenSQLTestsBaseTrait {
  private var registerQuotedConfigParserTest = false

  protected override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (isConfigParserCoverage(testName) && !registerQuotedConfigParserTest) {
      ()
    } else {
      super.test(testName, testTags: _*)(testFun)(pos)
    }
  }

  registerQuotedConfigParserTest = true
  test("Checks if SET/RESET can parse all the configurations") {
    sqlConf.getAllDefinedConfs.map(_._1).foreach {
      key: String =>
        val quotedKey = quoteConfigKey(key)
        spark.sessionState.sqlParser.parsePlan(s"SET $quotedKey")
        spark.sessionState.sqlParser.parsePlan(s"RESET $quotedKey")
    }
  }
  registerQuotedConfigParserTest = false

  private def isConfigParserCoverage(testName: String): Boolean = {
    testName == "Checks if SET/RESET can parse all the configurations"
  }

  private def quoteConfigKey(key: String): String = {
    s"`${key.replace("`", "``")}`"
  }
}
