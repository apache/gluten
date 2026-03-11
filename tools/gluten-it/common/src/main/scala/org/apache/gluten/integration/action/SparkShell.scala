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
package org.apache.gluten.integration.action

import org.apache.gluten.integration.{QueryRunner, Suite}

import org.apache.spark.repl.Main

case class SparkShell() extends Action {
  override def execute(suite: Suite): Boolean = {
    suite.sessionSwitcher.useSession("test", "Spark CLI")
    val runner: QueryRunner =
      new QueryRunner(suite.dataSource(), suite.dataWritePath())
    runner.createTables(suite.tableCreator(), suite.tableAnalyzer(), suite.sessionSwitcher.spark())
    Main.sparkSession = suite.sessionSwitcher.spark()
    Main.sparkContext = suite.sessionSwitcher.spark().sparkContext
    Main.main(Array("-usejavacp"))
    true
  }
}
