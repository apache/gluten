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

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec, ValidateRequirements}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable

/**
 * Gluten plan stability suites verify that Gluten-transformed plans satisfy Spark's distribution
 * and ordering requirements (via ValidateRequirements) and compare plans against Gluten-specific
 * golden files.
 *
 * Golden files are stored under: gluten-ut/sparkXX/src/test/resources/backends-{backendName}/
 * tpcds-plan-stability/gluten-approved-plans-{version}/{query}/{simplified,explain}.txt
 * gluten-tpch-plan-stability/{query}/{simplified,explain}.txt
 *
 * To generate or update golden files:
 * {{{
 *   export SPARK_GENERATE_GOLDEN_FILES=1
 *   export SPARK_ANSI_SQL_MODE=false
 *   export SPARK_TESTING=true
 *   export SPARK_HOME=/opt/shims/spark41/spark_home
 *   export SPARK_SCALA_VERSION=2.13
 *
 *   # Generate all 7 suites at once (recommended for consistency)
 *   ./dev/run-scala-test.sh --mvnd --clean \
 *     -Pjava-17,spark-4.1,scala-2.13,backends-velox,hadoop-3.3,spark-ut,delta \
 *     -pl gluten-ut/spark41 \
 *     -s org.apache.spark.sql.GlutenTPCDSV1_4_PlanStabilitySuite \
 *     -s org.apache.spark.sql.GlutenTPCDSV1_4_PlanStabilityWithStatsSuite \
 *     -s org.apache.spark.sql.GlutenTPCDSV2_7_PlanStabilitySuite \
 *     -s org.apache.spark.sql.GlutenTPCDSV2_7_PlanStabilityWithStatsSuite \
 *     -s org.apache.spark.sql.GlutenTPCDSModifiedPlanStabilitySuite \
 *     -s org.apache.spark.sql.GlutenTPCDSModifiedPlanStabilityWithStatsSuite \
 *     -s org.apache.spark.sql.GlutenTPCHPlanStabilitySuite
 * }}}
 *
 * For Spark 4.0, replace spark-4.1 with spark-4.0, spark41 with spark40, and SPARK_HOME
 * accordingly.
 *
 * Note: Running all suites together in one JVM is recommended to avoid ExprId normalization issues
 * where string constants (e.g., Brand#23 in TPCH q19) may collide with ExprId numbers.
 */
trait GlutenPlanStabilityTestTrait {
  self: PlanStabilitySuite =>

  private val referenceRegex = "#\\d+".r
  private val exprIdRegexp = "(?<prefix>(?<!id=)#)\\d+L?".r
  private val planIdRegex = "(?<prefix>(plan_id=|id=#))\\d+".r
  private val preExprRegex = "_pre_\\d+".r
  private val preExprIdRegex = "(?<prefix>_pre_)\\d+".r
  private val clsName = this.getClass.getCanonicalName

  private lazy val glutenGoldenFilePath: String = {
    // Resolve golden file path under backend-specific directory in Gluten's test resources.
    // goldenFilePath from parent is like:
    //   .../tpcds-plan-stability/approved-plans-v1_4  (TPCDS)
    //   .../tpch-plan-stability                       (TPCH)
    // We create backend-specific paths like:
    //   backends-velox/tpcds-plan-stability/gluten-approved-plans-v1_4

    // Find gluten-ut source resources via classpath, then map back to src/test/resources
    val resourceUrl = getClass.getClassLoader.getResource("log4j2.properties")
    val glutenResourceDir = if (resourceUrl != null) {
      val targetDir = new File(resourceUrl.toURI).getParentFile
      // targetDir is like: .../gluten-ut/sparkXX/target/scala-2.13/test-classes
      // We need:           .../gluten-ut/sparkXX/src/test/resources
      val moduleDir = targetDir.getParentFile.getParentFile.getParentFile
      new File(moduleDir, "src/test/resources")
    } else {
      new File("gluten-ut/spark40/src/test/resources")
    }

    // Compute relative path from SPARK_HOME resources to goldenFilePath
    val sparkResourcesDir = baseResourcePath.getParentFile // tpcds-plan-stability's parent
    val sparkGoldenPath = new File(goldenFilePath)
    val relativePath = sparkResourcesDir.toPath.relativize(sparkGoldenPath.toPath)
    // relativePath is like: tpcds-plan-stability/approved-plans-v1_4 or tpch-plan-stability
    val parts = relativePath.toString.split(File.separator).toSeq
    // Prefix the last part with "gluten-"
    val glutenParts = parts.init :+ s"gluten-${parts.last}"

    val backendName = BackendsApiManager.getBackendName
    val backendDir = new File(glutenResourceDir, s"backends-$backendName")
    new File(backendDir, glutenParts.mkString(File.separator)).getAbsolutePath
  }

  private def glutenGetDirForTest(name: String): File = {
    new File(glutenGoldenFilePath, name)
  }

  private def glutenIsApproved(
      dir: File,
      actualSimplifiedPlan: String,
      actualExplain: String): Boolean = {
    val simplifiedFile = new File(dir, "simplified.txt")
    if (!simplifiedFile.exists()) return false
    val expectedSimplified = Files.readString(simplifiedFile.toPath)
    lazy val explainFile = new File(dir, "explain.txt")
    lazy val expectedExplain = Files.readString(explainFile.toPath)
    expectedSimplified == actualSimplifiedPlan && expectedExplain == actualExplain
  }

  private def glutenGenerateGoldenFile(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = glutenGetDirForTest(name)
    val simplified = glutenGetSimplifiedPlan(plan)
    val foundMatch = dir.exists() && glutenIsApproved(dir, simplified, explain)

    if (!foundMatch) {
      Utils.deleteRecursively(dir)
      assert(Utils.createDirectory(dir))

      val file = new File(dir, "simplified.txt")
      Files.writeString(file.toPath(), simplified, StandardCharsets.UTF_8)
      val fileOriginalPlan = new File(dir, "explain.txt")
      Files.writeString(fileOriginalPlan.toPath(), explain, StandardCharsets.UTF_8)
    }
  }

  private def glutenCheckWithApproved(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = glutenGetDirForTest(name)
    val tempDir = System.getProperty("java.io.tmpdir")
    val actualSimplified = glutenGetSimplifiedPlan(plan)
    val foundMatch = glutenIsApproved(dir, actualSimplified, explain)

    if (!foundMatch) {
      val approvedSimplifiedFile = new File(dir, "simplified.txt")
      val approvedExplainFile = new File(dir, "explain.txt")

      val actualSimplifiedFile = new File(tempDir, s"$name.actual.simplified.txt")
      val actualExplainFile = new File(tempDir, s"$name.actual.explain.txt")

      val approvedSimplified = if (approvedSimplifiedFile.exists()) {
        Files.readString(approvedSimplifiedFile.toPath)
      } else {
        s"(golden file not found: ${approvedSimplifiedFile.getAbsolutePath})"
      }
      Files.writeString(actualSimplifiedFile.toPath(), actualSimplified, StandardCharsets.UTF_8)
      Files.writeString(actualExplainFile.toPath(), explain, StandardCharsets.UTF_8)

      fail(s"""
              |Plans did not match:
              |last approved simplified plan: ${approvedSimplifiedFile.getAbsolutePath}
              |last approved explain plan: ${approvedExplainFile.getAbsolutePath}
              |
              |$approvedSimplified
              |
              |actual simplified plan: ${actualSimplifiedFile.getAbsolutePath}
              |actual explain plan: ${actualExplainFile.getAbsolutePath}
              |
              |$actualSimplified
        """.stripMargin)
    }
  }

  private def glutenGetSimplifiedPlan(plan: SparkPlan): String = {
    val exchangeIdMap = new mutable.HashMap[Int, Int]()
    val subqueriesMap = new mutable.HashMap[Int, Int]()

    def getId(plan: SparkPlan): Int = plan match {
      // Gluten columnar exchanges (must be before Exchange since they extend Exchange)
      case exchange: ColumnarShuffleExchangeExecBase =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case exchange: ColumnarBroadcastExchangeExec =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case exchange: Exchange =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case ReusedExchangeExec(_, exchange) =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      // Gluten columnar subquery (must be before SubqueryExec)
      case subquery: ColumnarSubqueryBroadcastExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case subquery: SubqueryExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case subquery: SubqueryBroadcastExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case ReusedSubqueryExec(subquery) =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case _ => -1
    }

    def cleanUpReferences(references: AttributeSet): String = {
      val cleaned = referenceRegex.replaceAllIn(references.map(_.name).mkString(","), "")
      preExprRegex.replaceAllIn(cleaned, "_pre_x")
    }

    def simplifyNode(node: SparkPlan, depth: Int): String = {
      val padding = "  ".repeat(depth)
      var thisNode = node.nodeName
      if (node.references.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.references)}]"
      }
      if (node.producedAttributes.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.producedAttributes)}]"
      }
      val id = getId(node)
      if (id > 0) {
        thisNode += s" #$id"
      }
      val childrenSimplified = node.children.map(simplifyNode(_, depth + 1))
      val subqueriesSimplified = node.subqueries.map(simplifyNode(_, depth + 1))
      s"$padding$thisNode\n${subqueriesSimplified.mkString("")}${childrenSimplified.mkString("")}"
    }

    simplifyNode(plan, 0)
  }

  private def glutenNormalizeIds(plan: String): String = {
    val exprIdMap = new mutable.HashMap[String, String]()
    val exprIdNormalized = exprIdRegexp.replaceAllIn(
      plan,
      m => exprIdMap.getOrElseUpdate(m.toString(), s"${m.group("prefix")}${exprIdMap.size + 1}"))

    val planIdMap = new mutable.HashMap[String, String]()
    val planIdNormalized = planIdRegex.replaceAllIn(
      exprIdNormalized,
      m => planIdMap.getOrElseUpdate(s"$m", s"${m.group("prefix")}${planIdMap.size + 1}"))

    // Normalize _pre_N suffixes generated by Gluten's pre-projection optimization.
    // These internal expression names vary depending on session state.
    val preExprMap = new mutable.HashMap[String, String]()
    preExprIdRegex.replaceAllIn(
      planIdNormalized,
      m => preExprMap.getOrElseUpdate(m.toString(), s"${m.group("prefix")}${preExprMap.size + 1}"))
  }

  private def glutenNormalizeLocation(plan: String): String = {
    // Replace absolute paths in Location lines while preserving table names.
    // Handles both InMemoryFileIndex and CatalogFileIndex.
    // e.g., "Location: InMemoryFileIndex [file:/.../spark-warehouse/date_dim]"
    //     -> "Location: InMemoryFileIndex [{warehouse_dir}/date_dim]"
    plan.replaceAll(
      "(Location: (?:InMemoryFileIndex|CatalogFileIndex) \\[)file:.*?/spark-warehouse/",
      "$1{warehouse_dir}/")
  }

  override protected def testQuery(tpcdsGroup: String, query: String, suffix: String = ""): Unit = {
    val queryString = resourceToString(
      s"$tpcdsGroup/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    withSQLConf(
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
      SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val qe = sql(queryString).queryExecution
      val plan = qe.executedPlan
      val explain = glutenNormalizeLocation(glutenNormalizeIds(qe.explainString(FormattedMode)))

      assert(
        ValidateRequirements.validate(plan),
        s"ValidateRequirements failed for $tpcdsGroup/$query$suffix")

      if (regenerateGoldenFiles) {
        glutenGenerateGoldenFile(plan, query + suffix, explain)
      } else {
        glutenCheckWithApproved(plan, query + suffix, explain)
      }
    }
  }
}

class GlutenTPCDSV1_4_PlanStabilitySuite
  extends TPCDSV1_4_PlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSV1_4_PlanStabilityWithStatsSuite
  extends TPCDSV1_4_PlanStabilityWithStatsSuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSV2_7_PlanStabilitySuite
  extends TPCDSV2_7_PlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSV2_7_PlanStabilityWithStatsSuite
  extends TPCDSV2_7_PlanStabilityWithStatsSuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSModifiedPlanStabilitySuite
  extends TPCDSModifiedPlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCDSModifiedPlanStabilityWithStatsSuite
  extends TPCDSModifiedPlanStabilityWithStatsSuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}

class GlutenTPCHPlanStabilitySuite
  extends TPCHPlanStabilitySuite
  with GlutenSQLTestsBaseTrait
  with GlutenPlanStabilityTestTrait {}
