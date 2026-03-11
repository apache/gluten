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
package org.apache.spark.sql.statistics

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.sql.{GlutenTestConstants, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

import scala.util.control.Breaks.{break, breakable}

/**
 * TODO: There are some false positive & false negative cases for some functions. For such
 * situation, we need to use a suitable test sql to do the check.
 */
class SparkFunctionStatistics extends QueryTest {

  var spark: SparkSession = null

  protected def initializeSession(): Unit = {
    if (spark == null) {
      val sparkBuilder = SparkSession
        .builder()
        .appName("Gluten-UT")
        .master(s"local[2]")
        // Avoid static evaluation for literal input by spark catalyst.
        .config(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
          ConvertToLocalRelation.ruleName +
            "," + ConstantFolding.ruleName + "," + NullPropagation.ruleName)
        .config("spark.driver.memory", "1G")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1024MB")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        // Avoid the code size overflow error in Spark code generation.
        .config("spark.sql.codegen.wholeStage", "false")

      spark = if (BackendTestUtils.isCHBackendLoaded()) {
        sparkBuilder
          .config("spark.io.compression.codec", "LZ4")
          .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
          .config(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
          .config("spark.sql.files.openCostInBytes", "134217728")
          .config("spark.unsafe.exceptionOnMemoryLeak", "true")
          .getOrCreate()
      } else {
        sparkBuilder
          .config("spark.unsafe.exceptionOnMemoryLeak", "true")
          .getOrCreate()
      }
    }
  }

  def extractQuery(examples: String): Seq[String] = {
    examples
      .split("\n")
      .map(_.trim)
      .filter(!_.isEmpty)
      .filter(_.startsWith("> SELECT"))
      .map(_.replace("> SELECT", "SELECT"))
  }

  test(GlutenTestConstants.GLUTEN_TEST + "Run spark function statistics: ") {
    initializeSession
    val functionRegistry = spark.sessionState.functionRegistry
    val sparkBuiltInFunctions = functionRegistry.listFunction()
    // According to expressionsForTimestampNTZSupport in FunctionRegistry.scala,
    // these functions are registered only for testing, not available for end users.
    // Other functions like current_database is NOT necessarily offloaded to native.
    val ignoreFunctions = Seq(
      "get_fake_app_name",
      "current_catalog",
      "current_database",
      "spark_partition_id",
      "current_user",
      "current_timezone")
    val supportedFunctions = new java.util.ArrayList[String]()
    val unsupportedFunctions = new java.util.ArrayList[String]()
    val needInspectFunctions = new java.util.ArrayList[String]()

    for (func <- sparkBuiltInFunctions) {
      val exprInfo = functionRegistry.lookupFunction(func).get
      if (!ignoreFunctions.contains(exprInfo.getName)) {
        val examples = extractQuery(exprInfo.getExamples)
        if (examples.isEmpty) {
          needInspectFunctions.add(exprInfo.getName)
          // scalastyle:off println
          println("## Not found examples for " + exprInfo.getName)
          // scalastyle:on println
        }
        var isSupported: Boolean = true
        breakable {
          for (example <- examples) {
            var executedPlan: SparkPlan = null
            try {
              executedPlan = spark.sql(example).queryExecution.executedPlan
            } catch {
              case t: Throwable =>
                needInspectFunctions.add(exprInfo.getName)
                // scalastyle:off println
                println("-- Need inspect " + exprInfo.getName)
                println(exprInfo.getExamples)
                // scalastyle:on println
                break
            }
            val hasFallbackProject = executedPlan.find(_.isInstanceOf[ProjectExec]).isDefined
            if (hasFallbackProject) {
              isSupported = false
              break
            }
            val hasGlutenPlan = executedPlan.find(_.isInstanceOf[GlutenPlan]).isDefined
            if (!hasGlutenPlan) {
              isSupported = false
              break
            }
            break
          }
        }
        if (isSupported && !needInspectFunctions.contains(exprInfo.getName)) {
          supportedFunctions.add(exprInfo.getName)
        } else if (!isSupported) {
          unsupportedFunctions.add(exprInfo.getName)
        }
      }
    }
    // scalastyle:off println
    println("Overall functions: " + (sparkBuiltInFunctions.size - ignoreFunctions.size))
    println("Supported functions: " + supportedFunctions.size())
    println("Unsupported functions: " + unsupportedFunctions.size())
    println("Need inspect functions: " + needInspectFunctions.size())
    // scalastyle:on println
    // For correction.
    val supportedCastAliasFunctions = Seq(
      "boolean",
      "tinyint",
      "smallint",
      "int",
      "bigint",
      "float",
      "double",
      "decimal",
      "date",
      "binary",
      "string")
    for (func <- supportedCastAliasFunctions) {
      if (needInspectFunctions.contains(func)) {
        needInspectFunctions.remove(func)
        supportedFunctions.add(func)
      }
    }

    // For wrongly recognized unsupported case.
    Seq("%", "ceil", "floor", "first", "first_value", "last", "last_value", "hash", "mod").foreach(
      name => {
        if (unsupportedFunctions.remove(name)) {
          supportedFunctions.add(name)
        }
      })
    // For wrongly recognized supported case.
    Seq(
      "array_contains",
      "map_keys",
      "get_json_object",
      "element_at",
      "map_from_arrays",
      "contains",
      "startswith",
      "endswith",
      "map_contains_key",
      "map_values",
      "try_element_at",
      "struct",
      "array",
      "ilike",
      "sec",
      "csc"
    ).foreach(
      name => {
        if (supportedFunctions.remove(name)) {
          unsupportedFunctions.add(name)
        }
      })
    // Functions in needInspectFunctions were checked.
    unsupportedFunctions.addAll(needInspectFunctions)
    // scalastyle:off println
    println("---------------")
    println("Overall functions: " + (sparkBuiltInFunctions.size - ignoreFunctions.size))
    println("Supported functions corrected: " + supportedFunctions.size())
    println("Unsupported functions corrected: " + unsupportedFunctions.size())
    println("Support list:")
    println(supportedFunctions)
    println("Not support list:")
    println(unsupportedFunctions)
    // scalastyle:on println
  }
}
