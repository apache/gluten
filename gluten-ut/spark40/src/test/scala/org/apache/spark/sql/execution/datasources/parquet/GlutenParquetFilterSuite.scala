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
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.classic.ClassicColumn
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, HadoopFsRelation, LogicalRelation, PushableColumnAndNestedColumn}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.LegacyBehaviorPolicy.{CORRECTED, LEGACY}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType.INT96
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, Operators}
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.Operators.{Column => _, Eq, Gt, GtEq, Lt, LtEq, NotEq}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetOutputFormat}
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.sql.{Date, Timestamp}
import java.time.LocalDate

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

abstract class GlutenParquetFilterSuite extends ParquetFilterSuite with GlutenSQLTestsBaseTrait {
  protected def checkFilterPredicate(
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      expected: Seq[Row])(implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, filterClass, checkAnswer(_, _: Seq[Row]), expected)
  }

  protected def checkFilterPredicate[T](
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      expected: T)(implicit df: DataFrame): Unit = {
    checkFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }

  override protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(
      getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + name)
  }

  testGluten("filter pushdown - timestamp") {
    Seq(true, false).foreach {
      java8Api =>
        Seq(CORRECTED, LEGACY).foreach {
          rebaseMode =>
            val millisData = Seq(
              "1000-06-14 08:28:53.123",
              "1582-06-15 08:28:53.001",
              "1900-06-16 08:28:53.0",
              "2018-06-17 08:28:53.999")
            // INT96 doesn't support pushdown
            withSQLConf(
              SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
              SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> rebaseMode.toString,
              SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> INT96.toString
            ) {
              import testImplicits._
              withTempPath {
                file =>
                  millisData
                    .map(i => Tuple1(Timestamp.valueOf(i)))
                    .toDF
                    .write
                    .format(dataSourceName)
                    .save(file.getCanonicalPath)
                  readParquetFile(file.getCanonicalPath) {
                    df =>
                      val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
                      assertResult(None) {
                        createParquetFilters(schema).createFilter(sources.IsNull("_1"))
                      }
                  }
              }
            }
        }
    }
  }

  testGluten("SPARK-12218: 'Not' is included in Parquet filter pushdown") {
    import testImplicits._

    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      withTempPath {
        dir =>
          val path = s"${dir.getCanonicalPath}/table1"
          val df = (1 to 5).map(i => (i, (i % 2).toString)).toDF("a", "b")
          df.show()
          df.write.parquet(path)

          checkAnswer(
            spark.read.parquet(path).where("not (a = 2) or not(b in ('1'))"),
            (1 to 5).map(i => Row(i, (i % 2).toString)))

          checkAnswer(
            spark.read.parquet(path).where("not (a = 2 and b in ('1'))"),
            (1 to 5).map(i => Row(i, (i % 2).toString)))
      }
    }
  }

  testGluten("SPARK-23852: Broken Parquet push-down for partially-written stats") {
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      // parquet-1217.parquet contains a single column with values -1, 0, 1, 2 and null.
      // The row-group statistics include null counts, but not min and max values, which
      // triggers PARQUET-1217.

      val df = readResourceParquetFile("test-data/parquet-1217.parquet")

      // Will return 0 rows if PARQUET-1217 is not fixed.
      assert(df.where("col > 0").count() === 2)
    }
  }

  testGluten("SPARK-17091: Convert IN predicate to Parquet filter push-down") {
    val schema = StructType(
      Seq(
        StructField("a", IntegerType, nullable = false)
      ))

    val parquetSchema = new SparkToParquetSchemaConverter(conf).convert(schema)
    val parquetFilters = createParquetFilters(parquetSchema)
    assertResult(Some(FilterApi.eq(intColumn("a"), null: Integer))) {
      parquetFilters.createFilter(sources.In("a", Array(null)))
    }

    assertResult(Some(FilterApi.eq(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(sources.In("a", Array(10)))
    }

    // Remove duplicates
    assertResult(Some(FilterApi.eq(intColumn("a"), 10: Integer))) {
      parquetFilters.createFilter(sources.In("a", Array(10, 10)))
    }

    assertResult(
      Some(
        or(
          or(FilterApi.eq(intColumn("a"), 10: Integer), FilterApi.eq(intColumn("a"), 20: Integer)),
          FilterApi.eq(intColumn("a"), 30: Integer)))) {
      parquetFilters.createFilter(sources.In("a", Array(10, 20, 30)))
    }

    Seq(0, 10).foreach {
      threshold =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> threshold.toString) {
          assert(
            createParquetFilters(parquetSchema)
              .createFilter(sources.In("a", Array(10, 20, 30)))
              .nonEmpty === threshold > 0)
        }
    }

    import testImplicits._
    withTempPath {
      path =>
        val data = 0 to 1024
        data
          .toDF("a")
          .selectExpr("if (a = 1024, null, a) AS a") // convert 1024 to null
          .coalesce(1)
          .write
          .option("parquet.block.size", 512)
          .parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        Seq(true, false).foreach {
          pushEnabled =>
            withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> pushEnabled.toString) {
              Seq(1, 5, 10, 11, 100).foreach {
                count =>
                  val filter = s"a in(${Range(0, count).mkString(",")})"
                  assert(df.where(filter).count() === count)
                  val actual = stripSparkFilter(df.where(filter)).collect().length
                  assert(actual === count)
              }
              assert(df.where("a in(null)").count() === 0)
              assert(df.where("a = null").count() === 0)
              assert(df.where("a is null").count() === 1)
            }
        }
    }
  }

  // Velox doesn't support ParquetOutputFormat.PAGE_SIZE and ParquetOutputFormat.BLOCK_SIZE.
  ignoreGluten("Support Parquet column index") {
    // block 1:
    //                      null count  min                                       max
    // page-0                         0  0                                         99
    // page-1                         0  100                                       199
    // page-2                         0  200                                       299
    // page-3                         0  300                                       399
    // page-4                         0  400                                       449
    //
    // block 2:
    //                      null count  min                                       max
    // page-0                         0  450                                       549
    // page-1                         0  550                                       649
    // page-2                         0  650                                       749
    // page-3                         0  750                                       849
    // page-4                         0  850                                       899
    withTempPath {
      path =>
        spark
          .range(900)
          .repartition(1)
          .write
          .option(ParquetOutputFormat.PAGE_SIZE, "500")
          .option(ParquetOutputFormat.BLOCK_SIZE, "2000")
          .parquet(path.getCanonicalPath)

        val parquetFile = path.listFiles().filter(_.getName.startsWith("part")).last
        val in = HadoopInputFile.fromPath(
          new Path(parquetFile.getCanonicalPath),
          spark.sessionState.newHadoopConf())

        Utils.tryWithResource(ParquetFileReader.open(in)) {
          reader =>
            val blocks = reader.getFooter.getBlocks
            assert(blocks.size() > 1)
            val columns = blocks.get(0).getColumns
            assert(columns.size() === 1)
            val columnIndex = reader.readColumnIndex(columns.get(0))
            assert(columnIndex.getMinValues.size() > 1)

            val rowGroupCnt = blocks.get(0).getRowCount
            // Page count = Second page min value - first page min value
            val pageCnt = columnIndex.getMinValues.get(1).asLongBuffer().get() -
              columnIndex.getMinValues.get(0).asLongBuffer().get()
            assert(pageCnt < rowGroupCnt)
            Seq(true, false).foreach {
              columnIndex =>
                withSQLConf(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED -> s"$columnIndex") {
                  val df = spark.read.parquet(parquetFile.getCanonicalPath).where("id = 1")
                  df.collect()
                  val plan = df.queryExecution.executedPlan
                  // Ignore metrics comparison.
                  /*
            val metrics = plan.collectLeaves().head.metrics
            val numOutputRows = metrics("numOutputRows").value

            if (columnIndex) {
              assert(numOutputRows === pageCnt)
            } else {
              assert(numOutputRows === rowGroupCnt)
            }
                   */
                }
            }
        }
    }
  }
}

@ExtendedSQLTest
class GlutenParquetV1FilterSuite extends GlutenParquetFilterSuite with GlutenSQLTestsBaseTrait {
  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
  override def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    Seq(("parquet", true), ("", false)).foreach {
      case (pushdownDsList, nestedPredicatePushdown) =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> "true",
          // Disable adding filters from constraints because it adds, for instance,
          // is-not-null to pushed filters, which makes it hard to test if the pushed
          // filter is expected or not (this had to be fixed with SPARK-13495).
          SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName,
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false",
          SQLConf.NESTED_PREDICATE_PUSHDOWN_FILE_SOURCE_LIST.key -> pushdownDsList
        ) {
          val query = df
            .select(output.map(e => ClassicColumn(e)): _*)
            .where(ClassicColumn(predicate))

          val nestedOrAttributes = predicate.collectFirst {
            case g: GetStructField => g
            case a: Attribute => a
          }
          assert(nestedOrAttributes.isDefined, "No GetStructField nor Attribute is detected.")

          val parsed =
            parseColumnPath(PushableColumnAndNestedColumn.unapply(nestedOrAttributes.get).get)

          val containsNestedColumnOrDot = parsed.length > 1 || parsed(0).contains(".")

          var maybeRelation: Option[HadoopFsRelation] = None
          val maybeAnalyzedPredicate = query.queryExecution.optimizedPlan
            .collect {
              case PhysicalOperation(
                    _,
                    filters,
                    LogicalRelation(relation: HadoopFsRelation, _, _, _, _)) =>
                maybeRelation = Some(relation)
                filters
            }
            .flatten
            .reduceLeftOption(_ && _)
          assert(maybeAnalyzedPredicate.isDefined, "No filter is analyzed from the given query")

          val (_, selectedFilters, _) =
            DataSourceStrategy.selectFilters(maybeRelation.get, maybeAnalyzedPredicate.toSeq)
          // If predicates contains nested column or dot, we push down the predicates only if
          // "parquet" is in `NESTED_PREDICATE_PUSHDOWN_V1_SOURCE_LIST`.
          if (nestedPredicatePushdown || !containsNestedColumnOrDot) {
            assert(selectedFilters.nonEmpty, "No filter is pushed down")
            val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
            val parquetFilters = createParquetFilters(schema)
            // In this test suite, all the simple predicates are convertible here.
            assert(parquetFilters.convertibleFilters(selectedFilters) === selectedFilters)
            val pushedParquetFilters = selectedFilters.map {
              pred =>
                val maybeFilter = parquetFilters.createFilter(pred)
                assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
                maybeFilter.get
            }
            // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
            assert(
              pushedParquetFilters.exists(_.getClass === filterClass),
              s"${pushedParquetFilters.map(_.getClass).toList} did not contain $filterClass.")

            checker(stripSparkFilter(query), expected)
          } else {
            assert(selectedFilters.isEmpty, "There is filter pushed down")
          }
        }
    }
  }
}

@ExtendedSQLTest
class GlutenParquetV2FilterSuite extends GlutenParquetFilterSuite with GlutenSQLTestsBaseTrait {
  // TODO: enable Parquet V2 write path after file source V2 writers are workable.
  override def sparkConf: SparkConf =
    super.sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  override def checkFilterPredicate(
      df: DataFrame,
      predicate: Predicate,
      filterClass: Class[_ <: FilterPredicate],
      checker: (DataFrame, Seq[Row]) => Unit,
      expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    withSQLConf(
      SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> "true",
      // Disable adding filters from constraints because it adds, for instance,
      // is-not-null to pushed filters, which makes it hard to test if the pushed
      // filter is expected or not (this had to be fixed with SPARK-13495).
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName,
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false"
    ) {
      val query = df
        .select(output.map(e => ClassicColumn(e)): _*)
        .where(ClassicColumn(predicate))

      query.queryExecution.optimizedPlan.collectFirst {
        case PhysicalOperation(
              _,
              filters,
              DataSourceV2ScanRelation(_, scan: ParquetScan, _, None, None)) =>
          assert(filters.nonEmpty, "No filter is analyzed from the given query")
          val sourceFilters = filters.flatMap(DataSourceStrategy.translateFilter(_, true)).toArray
          val pushedFilters = scan.pushedFilters
          assert(pushedFilters.nonEmpty, "No filter is pushed down")
          val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
          val parquetFilters = createParquetFilters(schema)
          // In this test suite, all the simple predicates are convertible here.
          assert(parquetFilters.convertibleFilters(sourceFilters) === pushedFilters)
          val pushedParquetFilters = pushedFilters.map {
            pred =>
              val maybeFilter = parquetFilters.createFilter(pred)
              assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
              maybeFilter.get
          }
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          assert(
            pushedParquetFilters.exists(_.getClass === filterClass),
            s"${pushedParquetFilters.map(_.getClass).toList} did not contain $filterClass.")

          checker(stripSparkFilter(query), expected)

        case _ => assert(false, "Can not match ParquetTable in the query.")
      }
    }
  }

  /**
   * Takes a sequence of products `data` to generate multi-level nested dataframes as new test data.
   * It tests both non-nested and nested dataframes which are written and read back with Parquet
   * datasource.
   *
   * This is different from [[ParquetTest.withParquetDataFrame]] which does not test nested cases.
   */
  private def withNestedParquetDataFrame[T <: Product: ClassTag: TypeTag](data: Seq[T])(
      runTest: (DataFrame, String, Any => Any) => Unit): Unit =
    withNestedParquetDataFrame(spark.createDataFrame(data))(runTest)

  private def withNestedParquetDataFrame(inputDF: DataFrame)(
      runTest: (DataFrame, String, Any => Any) => Unit): Unit = {
    withNestedDataFrame(inputDF).foreach {
      case (newDF, colName, resultFun) =>
        withTempPath {
          file =>
            newDF.write.format(dataSourceName).save(file.getCanonicalPath)
            readParquetFile(file.getCanonicalPath)(df => runTest(df, colName, resultFun))
        }
    }
  }

  testGluten("filter pushdown - date") {
    implicit class StringToDate(s: String) {
      def date: Date = Date.valueOf(s)
    }

    val data = Seq("1000-01-01", "2018-03-19", "2018-03-20", "2018-03-21")
    import testImplicits._

    // Velox backend does not support rebaseMode being LEGACY.
    Seq(false, true).foreach {
      java8Api =>
        Seq(CORRECTED).foreach {
          rebaseMode =>
            withSQLConf(
              SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString,
              SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> rebaseMode.toString) {
              val dates = data.map(i => Tuple1(Date.valueOf(i))).toDF()
              withNestedParquetDataFrame(dates) {
                case (inputDF, colName, fun) =>
                  implicit val df: DataFrame = inputDF

                  def resultFun(dateStr: String): Any = {
                    val parsed = if (java8Api) LocalDate.parse(dateStr) else Date.valueOf(dateStr)
                    fun(parsed)
                  }

                  val dateAttr: Expression = df(colName).expr
                  assert(df(colName).expr.dataType === DateType)

                  checkFilterPredicate(dateAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
                  checkFilterPredicate(
                    dateAttr.isNotNull,
                    classOf[NotEq[_]],
                    data.map(i => Row.apply(resultFun(i))))

                  checkFilterPredicate(
                    dateAttr === "1000-01-01".date,
                    classOf[Eq[_]],
                    resultFun("1000-01-01"))
                  logWarning(s"java8Api: $java8Api, rebaseMode, $rebaseMode")
                  checkFilterPredicate(
                    dateAttr <=> "1000-01-01".date,
                    classOf[Eq[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    dateAttr =!= "1000-01-01".date,
                    classOf[NotEq[_]],
                    Seq("2018-03-19", "2018-03-20", "2018-03-21").map(i => Row.apply(resultFun(i))))

                  checkFilterPredicate(
                    dateAttr < "2018-03-19".date,
                    classOf[Lt[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    dateAttr > "2018-03-20".date,
                    classOf[Gt[_]],
                    resultFun("2018-03-21"))
                  checkFilterPredicate(
                    dateAttr <= "1000-01-01".date,
                    classOf[LtEq[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    dateAttr >= "2018-03-21".date,
                    classOf[GtEq[_]],
                    resultFun("2018-03-21"))

                  checkFilterPredicate(
                    Literal("1000-01-01".date) === dateAttr,
                    classOf[Eq[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    Literal("1000-01-01".date) <=> dateAttr,
                    classOf[Eq[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    Literal("2018-03-19".date) > dateAttr,
                    classOf[Lt[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    Literal("2018-03-20".date) < dateAttr,
                    classOf[Gt[_]],
                    resultFun("2018-03-21"))
                  checkFilterPredicate(
                    Literal("1000-01-01".date) >= dateAttr,
                    classOf[LtEq[_]],
                    resultFun("1000-01-01"))
                  checkFilterPredicate(
                    Literal("2018-03-21".date) <= dateAttr,
                    classOf[GtEq[_]],
                    resultFun("2018-03-21"))

                  checkFilterPredicate(
                    !(dateAttr < "2018-03-21".date),
                    classOf[GtEq[_]],
                    resultFun("2018-03-21"))
                  checkFilterPredicate(
                    dateAttr < "2018-03-19".date || dateAttr > "2018-03-20".date,
                    classOf[Operators.Or],
                    Seq(Row(resultFun("1000-01-01")), Row(resultFun("2018-03-21"))))

                  Seq(3, 20).foreach {
                    threshold =>
                      withSQLConf(
                        SQLConf.PARQUET_FILTER_PUSHDOWN_INFILTERTHRESHOLD.key -> s"$threshold") {
                        checkFilterPredicate(
                          In(
                            dateAttr,
                            Array(
                              "2018-03-19".date,
                              "2018-03-20".date,
                              "2018-03-21".date,
                              "2018-03-22".date).map(Literal.apply)),
                          if (threshold == 3) classOf[Operators.In[_]] else classOf[Operators.Or],
                          Seq(
                            Row(resultFun("2018-03-19")),
                            Row(resultFun("2018-03-20")),
                            Row(resultFun("2018-03-21")))
                        )
                      }
                  }
              }
            }
        }
    }
  }
}
