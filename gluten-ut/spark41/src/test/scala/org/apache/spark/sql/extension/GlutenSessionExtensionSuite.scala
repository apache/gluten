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
package org.apache.spark.sql.extension

import org.apache.gluten.extension.injector.InjectorControl
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.internal.StaticSQLConf.SPARK_SESSION_EXTENSIONS

class GlutenSessionExtensionSuite extends GlutenSQLTestsTrait {

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(SPARK_SESSION_EXTENSIONS.key, classOf[MyExtensions].getCanonicalName)
  }

  testGluten("test gluten extensions") {
    assert(
      spark.sessionState.columnarRules
        .exists(_.isInstanceOf[InjectorControl.DisablerAware]))

    assert(spark.sessionState.planner.strategies.contains(MySparkStrategy(spark)))
    assert(spark.sessionState.analyzer.extendedResolutionRules.contains(MyRule(spark)))
    assert(spark.sessionState.analyzer.postHocResolutionRules.contains(MyRule(spark)))
    assert(spark.sessionState.analyzer.extendedCheckRules.contains(MyCheckRule(spark)))
    assert(spark.sessionState.optimizer.batches.flatMap(_.rules).contains(MyRule(spark)))
    if (BackendTestUtils.isCHBackendLoaded()) {
      assert(spark.sessionState.sqlParser.isInstanceOf[InjectorControl.DisablerAware])
    } else {
      assert(spark.sessionState.sqlParser.isInstanceOf[MyParser])
    }
    assert(
      spark.sessionState.functionRegistry
        .lookupFunction(MyExtensions.myFunction._1)
        .isDefined)
    assert(
      spark.sessionState.columnarRules.contains(
        MyColumnarRule(PreRuleReplaceAddWithBrokenVersion(), MyPostRule())))
  }
}
