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
package org.apache.spark.sql

import org.apache.gluten.component.BoltDeltaComponent
import org.apache.gluten.extension.DeltaCDFScanRule
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.{IntegerType, StructType}

import org.mockito.Mockito.{mock, when}

class GlutenBoltDeltaComponentSuite extends SparkFunSuite {
  private val cdfKey = "spark.gluten.sql.columnar.backend.bolt.delta.enableChangeDataFeedScan"

  test("Bolt expands CDF during optimization by default and preserves output identity") {
    val conf = new SQLConf
    val (rule, relation) = optimizerRuleAndRelation(conf)
    val expanded = rule(relation)
    assert(expanded.isInstanceOf[LocalRelation])
    assert(expanded.output == relation.output)
  }

  test("Bolt CDF offload honors its explicit false setting") {
    val conf = new SQLConf
    conf.setConfString(cdfKey, "false")
    val (rule, relation) = optimizerRuleAndRelation(conf)
    assert(rule(relation) eq relation)
  }

  test("Bolt CDF offload reads the session setting when its existing rule runs") {
    val conf = new SQLConf
    conf.setConfString(cdfKey, "false")
    val (rule, relation) = optimizerRuleAndRelation(conf)
    assert(rule(relation) eq relation)
    conf.setConfString(cdfKey, "true")
    assert(rule(relation).isInstanceOf[LocalRelation])
    conf.setConfString(cdfKey, "false")
    assert(rule(relation) eq relation)
  }

  private def optimizerRuleAndRelation(conf: SQLConf): (Rule[LogicalPlan], LogicalRelation) = {
    val session = mock(classOf[SparkSession])
    val state = mock(classOf[SessionState])
    when(session.sessionState).thenReturn(state)
    when(state.conf).thenReturn(conf)
    val extensions = new SparkSessionExtensions
    new BoltDeltaComponent().injectRules(new Injector(extensions))
    val rules = extensions.buildOptimizerRules(session)
      .filter(_.ruleName == classOf[DeltaCDFScanRule].getName)
    assert(rules.size == 1)
    assert(extensions.buildResolutionRules(session).isEmpty)

    val cdf = mock(classOf[CDCReader.DeltaCDFRelation])
    when(cdf.startingVersion).thenReturn(None)
    when(cdf.schema).thenReturn(new StructType().add("id", IntegerType))
    val relation = LogicalRelation(cdf, isStreaming = false)
    (rules.head, relation)
  }
}
