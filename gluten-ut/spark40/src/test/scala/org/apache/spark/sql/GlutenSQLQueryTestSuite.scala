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

import org.apache.gluten.config.GlutenConfig
// import org.apache.gluten.exception.GlutenException
import org.apache.gluten.utils.{BackendTestSettings, BackendTestUtils}

import org.apache.spark.{SparkConf, TestUtils}
// import org.apache.spark.ErrorMessageFormat.MINIMAL
// import org.apache.spark.SparkThrowableHelper.getMessage
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.TimestampTypes
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
// import scala.util.control.NonFatal

/**
 * Originated from org.apache.spark.sql.SQLQueryTestSuite, with the following modifications:
 *   - Overwrite the generated golden files to remove failed queries.
 *   - Overwrite the generated golden files to update expected results for exception message
 *     mismatches, result order mismatches in non-order-sensitive queries, and minor precision scale
 *     mismatches.
 *   - Remove the AnalyzerTest as it's not within the scope of the Gluten project.
 *
 * NOTE: DO NOT simply copy-paste this file for supporting new Spark versions. SQLQueryTestSuite is
 * actively modified in Spark, so compare the difference and apply the necessary changes.
 */
@ExtendedSQLTest
class GlutenSQLQueryTestSuite
  extends QueryTest
  with SharedSparkSession
  with SQLHelper
  with SQLQueryTestHelper {

  import IntegratedUDFTestUtils._

  // ==== Start of modifications for Gluten. ====
  protected val baseResourcePath = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toFile
  }
  protected val testDataPath = new File(baseResourcePath, "test-data").getAbsolutePath

  protected val overwriteInputFilePath = new File(
    BackendTestSettings.instance.getSQLQueryTestSettings.getResourceFilePath,
    "inputs").getAbsolutePath

  protected val overwriteGoldenFilePath = new File(
    BackendTestSettings.instance.getSQLQueryTestSettings.getResourceFilePath,
    "results").getAbsolutePath
  protected val analyzerGoldenFilePath =
    new File(baseResourcePath, "analyzer-results").getAbsolutePath

  private val isCHBackend = BackendTestUtils.isCHBackendLoaded()

  // List of supported cases to run with a certain backend, in lower case.
  private val supportedList: Set[String] =
    BackendTestSettings.instance.getSQLQueryTestSettings.getSupportedSQLQueryTests ++
      BackendTestSettings.instance.getSQLQueryTestSettings.getOverwriteSQLQueryTests

  private val normalizeRegex = "#\\d+L?".r
  private val nodeNumberRegex = "[\\^*]\\(\\d+\\)".r
  private def normalizeIds(plan: String): String = {
    val normalizedPlan = nodeNumberRegex.replaceAllIn(plan, "")
    val map = new mutable.HashMap[String, String]()
    normalizeRegex
      .findAllMatchIn(normalizedPlan)
      .map(_.toString)
      .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
    normalizeRegex.replaceAllIn(normalizedPlan, regexMatch => s"#${map(regexMatch.toString)}")
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      // Fewer shuffle partitions to speed up testing.
      .set(SQLConf.SHUFFLE_PARTITIONS, 4)
      // use Java 8 time API to handle negative years properly
      .set(SQLConf.DATETIME_JAVA8API_ENABLED, true)
      // SPARK-39564: don't print out serde to avoid introducing complicated and error-prone
      // regex magic.
      .set("spark.test.noSerdeInExplain", "true")
      .setAppName("Gluten-UT")
      .set("spark.driver.memory", "1G")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "134217728")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set(GlutenConfig.SMALL_FILE_THRESHOLD.key, "0")

    if (isCHBackend) {
      conf
        .set("spark.io.compression.codec", "LZ4")
        .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
        .set(GlutenConfig.NATIVE_VALIDATION_ENABLED.key, "false")
        .set("spark.sql.files.openCostInBytes", "134217728")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
    } else {
      conf
        .set("spark.unsafe.exceptionOnMemoryLeak", "true")
        // Avoid static evaluation for literal input by spark catalyst.
        .set(
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
          ConvertToLocalRelation.ruleName +
            "," + ConstantFolding.ruleName + "," + NullPropagation.ruleName)
    }
    conf
  }

  /** List of test cases to ignore, in lower cases. */
  protected def ignoreList: Set[String] = Set(
    "ignored.sql", // Do NOT remove this one. It is here to test the ignore functionality.
    "explain-aqe.sql", // Explain is different in Gluten.
    "explain-cbo.sql", // Explain is different in Gluten.
    "explain.sql" // Explain is different in Gluten.
  ) ++ otherIgnoreList ++
    BackendTestSettings.instance.getSQLQueryTestSettings.getIgnoredSQLQueryTests

  /**
   * Returns the desired file path for results, given the input file. This is implemented as a
   * function because differente Suites extending this class may want their results files with
   * different names or in different locations.
   */
  protected def resultFileForInputFile(file: File): String = {
    file.getAbsolutePath.replace(overwriteInputFilePath, overwriteGoldenFilePath) + ".out"
  }

  protected lazy val listTestCases: Seq[TestCase] = {
    listFilesRecursively(new File(overwriteInputFilePath))
      .flatMap {
        file =>
          var resultFile = resultFileForInputFile(file)
          var analyzerResultFile =
            file.getAbsolutePath.replace(overwriteInputFilePath, overwriteGoldenFilePath) + ".out"
          // JDK-4511638 changes 'toString' result of Float/Double
          // JDK-8282081 changes DataTimeFormatter 'F' symbol
          if (Utils.isJavaVersionAtLeast21) {
            if (new File(resultFile + ".java21").exists()) resultFile += ".java21"
            if (new File(analyzerResultFile + ".java21").exists()) analyzerResultFile += ".java21"
          }
          val absPath = file.getAbsolutePath
          val testCaseName = absPath.stripPrefix(overwriteInputFilePath).stripPrefix(File.separator)

          // Create test cases of test types that depend on the input filename.
          val newTestCases: Seq[TestCase] =
            if (
              file.getAbsolutePath.startsWith(
                s"$overwriteInputFilePath${File.separator}udf${File.separator}postgreSQL")
            ) {
              Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map {
                udf =>
                  UDFPgSQLTestCase(s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
              }
            } else if (
              file.getAbsolutePath.startsWith(s"$overwriteInputFilePath${File.separator}udf")
            ) {
              Seq(TestScalaUDF("udf"), TestPythonUDF("udf"), TestScalarPandasUDF("udf")).map {
                udf => UDFTestCase(s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
              }
            } else if (
              file.getAbsolutePath.startsWith(s"$overwriteInputFilePath${File.separator}udaf")
            ) {
              Seq(TestGroupedAggPandasUDF("udaf")).map {
                udf => UDAFTestCase(s"$testCaseName - ${udf.prettyName}", absPath, resultFile, udf)
              }
            } else if (
              file.getAbsolutePath.startsWith(s"$overwriteInputFilePath${File.separator}udtf")
            ) {
              Seq(TestUDTFSet(AllTestUDTFs)).map {
                udtfSet =>
                  UDTFSetTestCase(s"$testCaseName - Python UDTFs", absPath, resultFile, udtfSet)
              }
            } else if (
              file.getAbsolutePath.startsWith(s"$overwriteInputFilePath${File.separator}postgreSQL")
            ) {
              PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
            } else if (
              file.getAbsolutePath.startsWith(s"$overwriteInputFilePath${File.separator}nonansi")
            ) {
              NonAnsiTestCase(testCaseName, absPath, resultFile) :: Nil
            } else if (
              file.getAbsolutePath.startsWith(
                s"$overwriteInputFilePath${File.separator}timestampNTZ")
            ) {
              TimestampNTZTestCase(testCaseName, absPath, resultFile) :: Nil
            } else if (
              file.getAbsolutePath.startsWith(s"$overwriteInputFilePath${File.separator}cte.sql")
            ) {
              CTETestCase(testCaseName, absPath, resultFile) :: Nil
            } else {
              RegularTestCase(testCaseName, absPath, resultFile) :: Nil
            }
          // Also include a copy of each of the above test cases as an analyzer test.
          newTestCases.flatMap {
            test =>
              test match {
                case _: UDAFTestCase =>
                  // Skip creating analyzer test.
                  Seq(test)
                case _ =>
                  Seq(
                    test,
                    test.asAnalyzerTest(
                      newName = s"${test.name}_analyzer_test",
                      newResultFile = analyzerResultFile))
              }
          }
      }
      .sortBy(_.name)
  }
  // ==== End of modifications for Gluten. ====

  protected val inputFilePath = new File(baseResourcePath, "inputs").getAbsolutePath
  protected val goldenFilePath = new File(baseResourcePath, "results").getAbsolutePath

  // SPARK-32106 Since we add SQL test 'transform.sql' will use `cat` command,
  // here we need to ignore it.
  private val otherIgnoreList =
    if (TestUtils.testCommandAvailable("/bin/bash")) Nil else Set("transform.sql")

  // Create all the test cases.
  listTestCases.foreach(createScalaTestCase)

  /** A single SQL query's output. */
  protected case class QueryOutput(sql: String, schema: String, output: String) {
    override def toString: String = {
      // We are explicitly not using multi-line string due to stripMargin removing "|" in output.
      s"-- !query\n" +
        sql + "\n" +
        s"-- !query schema\n" +
        schema + "\n" +
        s"-- !query output\n" +
        output
    }
  }

  protected def createScalaTestCase(testCase: TestCase): Unit = {
    if (
      // Modified for Gluten to use exact name matching.
      !supportedList.exists(
        t => testCase.name.toLowerCase(Locale.ROOT) == t.toLowerCase(Locale.ROOT)) ||
      ignoreList.exists(t => testCase.name.toLowerCase(Locale.ROOT) == t.toLowerCase(Locale.ROOT))
    ) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      testCase match {
        case udfTestCase: UDFTest
            if udfTestCase.udf.isInstanceOf[TestPythonUDF] && !shouldTestPythonUDFs =>
          ignore(
            s"${testCase.name} is skipped because " +
              s"[$pythonExec] and/or pyspark were not available.") {
            /* Do nothing */
          }
        case udfTestCase: UDFTest
            if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && !shouldTestPandasUDFs =>
          ignore(
            s"${testCase.name} is skipped because pyspark," +
              s"pandas and/or pyarrow were not available in [$pythonExec].") {
            /* Do nothing */
          }
        case udfTestCase: UDFTest
            if udfTestCase.udf.isInstanceOf[TestGroupedAggPandasUDF] &&
              !shouldTestPandasUDFs =>
          ignore(
            s"${testCase.name} is skipped because pyspark," +
              s"pandas and/or pyarrow were not available in [$pythonExec].") {
            /* Do nothing */
          }
        case _ =>
          // Create a test case to run this case.
          test(testCase.name) {
            runSqlTestCase(testCase, listTestCases)
          }
      }
    }
  }

  /** Run a test case. */
  protected def runSqlTestCase(testCase: TestCase, listTestCases: Seq[TestCase]): Unit = {
    def splitWithSemicolon(seq: Seq[String]) = {
      seq.mkString("\n").split("(?<=[^\\\\]);")
    }

    def splitCommentsAndCodes(input: String) = input.split("\n").partition {
      line =>
        val newLine = line.trim
        newLine.startsWith("--") && !newLine.startsWith("--QUERY-DELIMITER")
    }

    val input = Files.readString(new File(testCase.inputFile).toPath)

    val (comments, code) = splitCommentsAndCodes(input)

    // If `--IMPORT` found, load code from another test case file, then insert them
    // into the head in this test.
    val importedTestCaseName = comments.filter(_.startsWith("--IMPORT ")).map(_.substring(9))
    val importedCode = importedTestCaseName.flatMap {
      testCaseName =>
        listTestCases.find(_.name == testCaseName).map {
          testCase =>
            val input = Files.readString(new File(testCase.inputFile).toPath)
            val (_, code) = splitCommentsAndCodes(input)
            code
        }
    }.flatten

    val allCode = importedCode ++ code
    val tempQueries = if (allCode.exists(_.trim.startsWith("--QUERY-DELIMITER"))) {
      // Although the loop is heavy, only used for bracketed comments test.
      val queries = new ArrayBuffer[String]
      val otherCodes = new ArrayBuffer[String]
      var tempStr = ""
      var start = false
      for (c <- allCode) {
        if (c.trim.startsWith("--QUERY-DELIMITER-START")) {
          start = true
          queries ++= splitWithSemicolon(otherCodes.toSeq)
          otherCodes.clear()
        } else if (c.trim.startsWith("--QUERY-DELIMITER-END")) {
          start = false
          queries += s"\n${tempStr.stripSuffix(";")}"
          tempStr = ""
        } else if (start) {
          tempStr += s"\n$c"
        } else {
          otherCodes += c
        }
      }
      if (otherCodes.nonEmpty) {
        queries ++= splitWithSemicolon(otherCodes.toSeq)
      }
      queries.toSeq
    } else {
      splitWithSemicolon(allCode).toSeq
    }

    // List of SQL queries to run
    val queries = tempQueries
      .map(_.trim)
      .filter(_ != "")
      .toSeq
      // Fix misplacement when comment is at the end of the query.
      .map(_.split("\n").filterNot(_.startsWith("--")).mkString("\n"))
      .map(_.trim)
      .filter(_ != "")

    val settingLines = comments.filter(_.startsWith("--SET ")).map(_.substring(6))
    val settings = settingLines.flatMap(_.split(",").map {
      kv =>
        val (conf, value) = kv.span(_ != '=')
        conf.trim -> value.substring(1).trim
    })

    if (regenerateGoldenFiles) {
      runQueries(queries, testCase, settings)
    } else {
      // A config dimension has multiple config sets, and a config set has multiple configs.
      // - config dim:     Seq[Seq[(String, String)]]
      //   - config set:   Seq[(String, String)]
      //     - config:     (String, String))
      // We need to do cartesian product for all the config dimensions, to get a list of
      // config sets, and run the query once for each config set.
      val configDimLines = comments.filter(_.startsWith("--CONFIG_DIM")).map(_.substring(12))
      val configDims = configDimLines.groupBy(_.takeWhile(_ != ' ')).mapValues {
        lines =>
          lines
            .map(_.dropWhile(_ != ' ').substring(1))
            .map(_.split(",")
              .map {
                kv =>
                  val (conf, value) = kv.span(_ != '=')
                  conf.trim -> value.substring(1).trim
              }
              .toSeq)
            .toSeq
      }

      val configSets = configDims.values.foldLeft(Seq(Seq[(String, String)]())) {
        (res, dim) => dim.flatMap(configSet => res.map(_ ++ configSet))
      }

      configSets.foreach {
        configSet =>
          try {
            runQueries(queries, testCase, settings ++ configSet)
          } catch {
            case e: Throwable =>
              val configs = configSet.map { case (k, v) => s"$k=$v" }
              logError(s"Error using configs: ${configs.mkString(",")}")
              throw e
          }
      }
    }
  }

  def hasNoDuplicateColumns(schema: String): Boolean = {
    val columnAndTypes = schema.replaceFirst("^struct<", "").stripSuffix(">").split(",")
    columnAndTypes.size == columnAndTypes.distinct.size
  }

  def expandCTEQueryAndCompareResult(
      session: SparkSession,
      query: String,
      output: ExecutionOutput): Unit = {
    val triggerCreateViewTest =
      try {
        val logicalPlan: LogicalPlan = session.sessionState.sqlParser.parsePlan(query)
        !logicalPlan.isInstanceOf[Command] &&
        output.schema.get != emptySchema &&
        hasNoDuplicateColumns(output.schema.get)
      } catch {
        case _: ParseException => return
      }

    // For non-command query with CTE, compare the results of selecting from view created on the
    // original query.
    if (triggerCreateViewTest) {
      val createView = s"CREATE temporary VIEW cte_view AS $query"
      val selectFromView = "SELECT * FROM cte_view"
      val dropViewIfExists = "DROP VIEW IF EXISTS cte_view"
      session.sql(createView)
      val (selectViewSchema, selectViewOutput) =
        handleExceptions(getNormalizedQueryExecutionResult(session, selectFromView))
      // Compare results.
      assertResult(
        output.schema.get,
        s"Schema did not match for CTE query and select from its view: \n$output") {
        selectViewSchema
      }
      assertResult(
        output.output,
        s"Result did not match for CTE query and select from its view: \n${output.sql}") {
        selectViewOutput.mkString("\n").replaceAll("\\s+$", "")
      }
      // Drop view.
      session.sql(dropViewIfExists)
    }
  }

  protected def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      configSet: Seq[(String, String)]): Unit = {
    // Create a local SparkSession to have stronger isolation between different test cases.
    // This does not isolate catalog changes.
    val localSparkSession = spark.newSession()

    testCase match {
      case udfTestCase: SQLQueryTestSuite#UDFTest =>
        registerTestUDF(udfTestCase.udf, localSparkSession)
      case udtfTestCase: SQLQueryTestSuite#UDTFSetTest =>
        registerTestUDTFs(udtfTestCase.udtfSet, localSparkSession)
      case _ =>
    }

    testCase match {
      case _: SQLQueryTestSuite#PgSQLTest =>
        // booleq/boolne used by boolean.sql
        localSparkSession.udf.register("booleq", (b1: Boolean, b2: Boolean) => b1 == b2)
        localSparkSession.udf.register("boolne", (b1: Boolean, b2: Boolean) => b1 != b2)
        // vol used by boolean.sql and case.sql.
        localSparkSession.udf.register("vol", (s: String) => s)
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
        localSparkSession.conf.set(SQLConf.LEGACY_INTERVAL_ENABLED.key, true)
      case _: SQLQueryTestSuite#NonAnsiTest =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, false)
      case _: SQLQueryTestSuite#TimestampNTZTest =>
        localSparkSession.conf.set(
          SQLConf.TIMESTAMP_TYPE.key,
          TimestampTypes.TIMESTAMP_NTZ.toString)
      case _ =>
        localSparkSession.conf.set(SQLConf.ANSI_ENABLED.key, true)
    }

    if (configSet.nonEmpty) {
      // Execute the list of set operation in order to add the desired configs
      val setOperations = configSet.map { case (key, value) => s"set $key=$value" }
      logInfo(s"Setting configs: ${setOperations.mkString(", ")}")
      setOperations.foreach(localSparkSession.sql)
    }

    // Run the SQL queries preparing them for comparison.
    val outputs: Seq[QueryTestOutput] = queries.map {
      sql =>
        testCase match {
          case _: AnalyzerTest =>
            val (_, output) =
              handleExceptions(getNormalizedQueryAnalysisResult(localSparkSession, sql))
            // We might need to do some query canonicalization in the future.
            AnalyzerOutput(
              sql = sql,
              schema = None,
              output = output.mkString("\n").replaceAll("\\s+$", ""))
          case _ =>
            val (schema, output) =
              handleExceptions(getNormalizedQueryExecutionResult(localSparkSession, sql))
            // We might need to do some query canonicalization in the future.
            val executionOutput = ExecutionOutput(
              sql = sql,
              schema = Some(schema),
              // GLUTEN-3559: Overwrite scalar-subquery-select.sql test
              output = normalizeIds(output.mkString("\n").replaceAll("\\s+$", ""))
            )
            if (testCase.isInstanceOf[CTETest]) {
              expandCTEQueryAndCompareResult(localSparkSession, sql, executionOutput)
            }
            executionOutput
        }
    }

    if (regenerateGoldenFiles) {
      // Again, we are explicitly not using multi-line string due to stripMargin removing "|".
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n" +
          outputs.mkString("\n\n\n") + "\n"
      }
      val resultFile = new File(testCase.resultFile)
      val parent = resultFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(resultFile, goldenOutput)
    }

    // This is a temporary workaround for SPARK-28894. The test names are truncated after
    // the last dot due to a bug in SBT. This makes easier to debug via Jenkins test result
    // report. See SPARK-28894.
    // See also SPARK-29127. It is difficult to see the version information in the failed test
    // cases so the version information related to Python was also added.
    val clue = testCase match {
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestPythonUDF] && shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestScalarPandasUDF] && shouldTestPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case udfTestCase: SQLQueryTestSuite#UDFTest
          if udfTestCase.udf.isInstanceOf[TestGroupedAggPandasUDF] &&
            shouldTestPandasUDFs =>
        s"${testCase.name}${System.lineSeparator()}" +
          s"Python: $pythonVer Pandas: $pandasVer PyArrow: $pyarrowVer${System.lineSeparator()}"
      case udtfTestCase: SQLQueryTestSuite#UDTFSetTest
          if udtfTestCase.udtfSet.udtfs.forall(_.isInstanceOf[TestPythonUDTF]) &&
            shouldTestPythonUDFs =>
        s"${testCase.name}${System.lineSeparator()}Python: $pythonVer${System.lineSeparator()}"
      case _ =>
        s"${testCase.name}${System.lineSeparator()}"
    }

    withClue(clue) {
      testCase match {
        case _: AnalyzerTest =>
          readGoldenFileAndCompareResults(testCase.resultFile, outputs, AnalyzerOutput)
        case _ =>
          readGoldenFileAndCompareResults(testCase.resultFile, outputs, ExecutionOutput)
      }
    }
  }

  /** Load built-in test tables into the SparkSession. */
  private def createTestTables(session: SparkSession): Unit = {
    import session.implicits._

    // Before creating test tables, deletes orphan directories in warehouse dir
    Seq("testdata", "arraydata", "mapdata", "aggtest", "onek", "tenk1").foreach {
      dirName =>
        val f = new File(new URI(s"${conf.warehousePath}/$dirName"))
        if (f.exists()) {
          Utils.deleteRecursively(f)
        }
    }

    (1 to 100)
      .map(i => (i, i.toString))
      .toDF("key", "value")
      .repartition(1)
      .write
      .format("parquet")
      .saveAsTable("testdata")

    ((Seq(1, 2, 3), Seq(Seq(1, 2, 3))) :: (Seq(2, 3, 4), Seq(Seq(2, 3, 4))) :: Nil)
      .toDF("arraycol", "nestedarraycol")
      .write
      .format("parquet")
      .saveAsTable("arraydata")

    (Tuple1(Map(1 -> "a1", 2 -> "b1", 3 -> "c1", 4 -> "d1", 5 -> "e1")) ::
      Tuple1(Map(1 -> "a2", 2 -> "b2", 3 -> "c2", 4 -> "d2")) ::
      Tuple1(Map(1 -> "a3", 2 -> "b3", 3 -> "c3")) ::
      Tuple1(Map(1 -> "a4", 2 -> "b4")) ::
      Tuple1(Map(1 -> "a5")) :: Nil)
      .toDF("mapcol")
      .write
      .format("parquet")
      .saveAsTable("mapdata")

    session.read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("a int, b float")
      .load(testDataPath + "/postgresql/agg.data")
      .write
      .format("parquet")
      .saveAsTable("aggtest")

    session.read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("""
                |unique1 int,
                |unique2 int,
                |two int,
                |four int,
                |ten int,
                |twenty int,
                |hundred int,
                |thousand int,
                |twothousand int,
                |fivethous int,
                |tenthous int,
                |odd int,
                |even int,
                |stringu1 string,
                |stringu2 string,
                |string4 string
        """.stripMargin)
      .load(testDataPath + "/postgresql/onek.data")
      .write
      .format("parquet")
      .saveAsTable("onek")

    session.read
      .format("csv")
      .options(Map("delimiter" -> "\t", "header" -> "false"))
      .schema("""
                |unique1 int,
                |unique2 int,
                |two int,
                |four int,
                |ten int,
                |twenty int,
                |hundred int,
                |thousand int,
                |twothousand int,
                |fivethous int,
                |tenthous int,
                |odd int,
                |even int,
                |stringu1 string,
                |stringu2 string,
                |string4 string
        """.stripMargin)
      .load(testDataPath + "/postgresql/tenk.data")
      .write
      .format("parquet")
      .saveAsTable("tenk1")
  }

  protected def removeTestTables(session: SparkSession): Unit = {
    session.sql("DROP TABLE IF EXISTS testdata")
    session.sql("DROP TABLE IF EXISTS arraydata")
    session.sql("DROP TABLE IF EXISTS mapdata")
    session.sql("DROP TABLE IF EXISTS aggtest")
    session.sql("DROP TABLE IF EXISTS onek")
    session.sql("DROP TABLE IF EXISTS tenk1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTestTables(spark)
    RuleExecutor.resetMetrics()
    CodeGenerator.resetCompileTime()
    WholeStageCodegenExec.resetCodeGenTime()
  }

  override def afterAll(): Unit = {
    try {
      removeTestTables(spark)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())

      val codeGenTime = WholeStageCodegenExec.codeGenTime.toDouble / NANOS_PER_SECOND
      val compileTime = CodeGenerator.compileTime.toDouble / NANOS_PER_SECOND
      val codegenInfo =
        s"""
           |=== Metrics of Whole-stage Codegen ===
           |Total code generation time: $codeGenTime seconds
           |Total compile time: $compileTime seconds
         """.stripMargin
      logWarning(codegenInfo)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Consumes contents from a single golden file and compares the expected results against the
   * output of running a query.
   */
  def readGoldenFileAndCompareResults(
      resultFile: String,
      outputs: Seq[QueryTestOutput],
      makeOutput: (String, Option[String], String) => QueryTestOutput): Unit = {
    // Read back the golden file.
    val expectedOutputs: Seq[QueryTestOutput] = {
      val goldenOutput = Files.readString(new File(resultFile).toPath)
      val segments = goldenOutput.split("-- !query.*\n")

      val numSegments = outputs.map(_.numSegments).sum + 1
      assert(
        segments.size == numSegments,
        s"Expected $numSegments blocks in result file but got " +
          s"${segments.size}. Try regenerate the result files.")
      var curSegment = 0
      outputs.map {
        output =>
          val result = if (output.numSegments == 3) {
            makeOutput(
              segments(curSegment + 1).trim, // SQL
              Some(segments(curSegment + 2).trim), // Schema
              segments(curSegment + 3).replaceAll("\\s+$", "")
            ) // Output
          } else {
            makeOutput(
              segments(curSegment + 1).trim, // SQL
              None, // Schema
              segments(curSegment + 2).replaceAll("\\s+$", "")
            ) // Output
          }
          curSegment += output.numSegments
          result
      }
    }

    // Compare results.
    assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
      outputs.size
    }

    outputs.zip(expectedOutputs).zipWithIndex.foreach {
      case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }
        assertResult(
          expected.schema,
          s"Schema did not match for query #$i\n${expected.sql}: $output") {
          output.schema
        }
        assertResult(
          expected.output,
          s"Result did not match" +
            s" for query #$i\n${expected.sql}") {
          output.output
        }
    }
  }
}
