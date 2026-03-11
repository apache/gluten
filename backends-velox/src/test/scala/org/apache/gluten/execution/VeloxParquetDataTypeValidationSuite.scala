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
package org.apache.gluten.execution

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf

import java.io.File

class VeloxParquetDataTypeValidationSuite extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/data-type-validation-data"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createDataTypeTable()
  }

  protected def createDataTypeTable(): Unit = {
    TPCHTableDataFrames = Seq(
      "type1",
      "type2"
    ).map {
      table =>
        val tableDir = getClass.getResource(resourcePath).getFile
        val tablePath = new File(tableDir, table).getAbsolutePath
        val tableDF = spark.read.format(fileFormat).load(tablePath)
        tableDF.createOrReplaceTempView(table)
        (table, tableDF)
    }.toMap
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  test("Bool type") {
    runQueryAndCompare("select bool from type1 limit 1") { _ => }

    // Validation: BatchScan Filter Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, bool from type1 where bool == true  " +
        " group by grouping sets(int, bool) sort by int, bool limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.bool from type1," +
        " type2 where type1.bool = type2.bool") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.bool from type1," +
        " type2 where type1.bool = type2.bool") { _ => }
  }

  test("Binary type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, binary from type1 " +
        " group by grouping sets(int, binary) sort by int, binary limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.binary from type1," +
        " type2 where type1.binary = type2.binary") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.binary from type1," +
        " type2 where type1.binary = type2.binary") { _ => }
  }

  test("String type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, string from type1" +
        " group by grouping sets(int, string) sort by int, string limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.string from type1," +
        " type2 where type1.string = type2.string") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.string from type1," +
        " type2 where type1.string = type2.string") { _ => }
  }

  test("Double type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, double from type1 " +
        " group by grouping sets(int, double) sort by int, double limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.double from type1," +
        " type2 where type1.double = type2.double") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.double from type1," +
        " type2 where type1.double = type2.double") { _ => }
  }

  test("Float type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, float from type1 " +
        " group by grouping sets(int, float) sort by int, float limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.float from type1," +
        " type2 where type1.float = type2.float") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.float from type1," +
        " type2 where type1.float = type2.float") { _ => }
  }

  test("Long type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, long from type1 " +
        " group by grouping sets(int, long) sort by int, long limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.long from type1," +
        " type2 where type1.long = type2.long") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.long from type1," +
        " type2 where type1.long = type2.long") { _ => }
  }

  test("Int type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, short from type1 " +
        " group by grouping sets(int, short) sort by short, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.int from type1," +
        " type2 where type1.int = type2.int") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.int from type1," +
        " type2 where type1.int = type2.int") { _ => }
  }

  test("Short type") {
    // Validation: BatchScan with Filter
    runQueryAndCompare(
      "select type1.short, int from type1" +
        " where type1.short = 1",
      false) {
      checkGlutenPlan[BatchScanExecTransformer]
    }

    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, short from type1 " +
        " group by grouping sets(int, short) sort by short, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.short from type1," +
        " type2 where type1.short = type2.short") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.short from type1," +
        " type2 where type1.short = type2.short") { _ => }
  }

  test("Date type") {
    // Validation: BatchScan, Project, Aggregate, Sort.
    runQueryAndCompare(
      "select int, date from type1 " +
        " group by grouping sets(int, date) sort by date, int limit 1") {
      df =>
        {
          val executedPlan = getExecutedPlan(df)
          assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
          assert(executedPlan.exists(plan => plan.isInstanceOf[ProjectExecTransformer]))
          assert(executedPlan.exists(plan => plan.isInstanceOf[HashAggregateExecTransformer]))
          assert(executedPlan.exists(plan => plan.isInstanceOf[SortExecTransformer]))
        }
    }

    // Validation: Expand, Filter.
    runQueryAndCompare(
      "select date, string, sum(int) from type1 where date > date '1990-01-09' " +
        "group by rollup(date, string) order by date, string") {
      df =>
        {
          val executedPlan = getExecutedPlan(df)
          assert(executedPlan.exists(plan => plan.isInstanceOf[ExpandExecTransformer]))
          assert(executedPlan.exists(plan => plan.isInstanceOf[FilterExecTransformer]))
        }
    }

    // Validation: Union.
    runQueryAndCompare("""
                         |select count(d) from (
                         | select date as d from type1
                         | union all
                         | select date as d from type1
                         |);
                         |""".stripMargin) {
      df =>
        {
          assert(
            getExecutedPlan(df).exists(
              plan =>
                plan.isInstanceOf[ColumnarUnionExec] || plan.isInstanceOf[UnionExecTransformer]))
        }
    }

    // Validation: Limit.
    runQueryAndCompare("""
                         |select date, int from (
                         | select date, int from type1 limit 100
                         |) where int != 0 limit 10;
                         |""".stripMargin) {
      checkGlutenPlan[LimitExecTransformer]
    }

    // Validation: Window.
    runQueryAndCompare(
      "select row_number() over (partition by date order by int) from type1 order by int, date") {
      checkGlutenPlan[WindowExecTransformer]
    }

    // Validation: BroadHashJoin.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "10M") {
      runQueryAndCompare(
        "select type1.date from type1," +
          " type2 where type1.date = type2.date") {
        checkGlutenPlan[BroadcastHashJoinExecTransformer]
      }
    }

    // Validation: ShuffledHashJoin.
    withSQLConf(
      GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      runQueryAndCompare(
        "select type1.date from type1," +
          " type2 where type1.date = type2.date") {
        checkGlutenPlan[ShuffledHashJoinExecTransformer]
      }
    }

    // Validation: SortMergeJoin.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf(GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
        runQueryAndCompare(
          "select type1.date from type1," +
            " type2 where type1.date = type2.date") {
          checkGlutenPlan[SortMergeJoinExecTransformer]
        }
      }
    }
  }

  test("Byte type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, byte from type1 " +
        " group by grouping sets(int, byte) sort by byte, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.byte from type1," +
        " type2 where type1.byte = type2.byte") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.byte from type1," +
        " type2 where type1.byte = type2.byte") { _ => }
  }

  test("Array type") {
    withSQLConf(("spark.gluten.sql.complexType.scan.fallback.enabled", "false")) {
      // Validation: BatchScan.
      runQueryAndCompare("select array from type1") {
        checkGlutenPlan[BatchScanExecTransformer]
      }

      // Validation: BatchScan Project Aggregate Expand Sort Limit
      runQueryAndCompare(
        "select int, array from type1 " +
          " group by grouping sets(int, array) sort by array, int limit 1") {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
          }
      }

      // Validation: BroadHashJoin, Filter, Project
      super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
      runQueryAndCompare(
        "select type1.array from type1," +
          " type2 where type1.array = type2.array") { _ => }

      // Validation: ShuffledHashJoin, Filter, Project
      super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      runQueryAndCompare(
        "select type1.array from type1," +
          " type2 where type1.array = type2.array") { _ => }
    }
  }

  test("Map type") {
    withSQLConf(("spark.gluten.sql.complexType.scan.fallback.enabled", "false")) {
      // Validation: BatchScan Project Limit
      runQueryAndCompare("select map from type1 limit 1") {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
          }
      }
      // Validation: BatchScan Project Aggregate Sort Limit
      // TODO validate Expand operator support map type ?
      runQueryAndCompare(
        "select map['key'] from type1 group by map['key']" +
          " sort by map['key'] limit 1") {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
          }
      }

      // Validation: BroadHashJoin, Filter, Project
      super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
      runQueryAndCompare(
        "select type1.map['key'] from type1," +
          " type2 where type1.map['key'] = type2.map['key']") { _ => }

      // Validation: ShuffledHashJoin, Filter, Project
      super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      runQueryAndCompare(
        "select type1.map['key'] from type1," +
          " type2 where type1.map['key'] = type2.map['key']") { _ => }
    }
  }

  test("Struct type") {
    withSQLConf(("spark.gluten.sql.complexType.scan.fallback.enabled", "false")) {
      // Validation: BatchScan Project Limit
      runQueryAndCompare("select struct from type1") {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
          }
      }
      // Validation: BatchScan Project Aggregate Sort Limit
      // TODO validate Expand operator support Struct type ?
      runQueryAndCompare(
        "select int, struct.struct_1 from type1 " +
          "sort by struct.struct_1 limit 1") {
        df =>
          {
            val executedPlan = getExecutedPlan(df)
            assert(executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
            assert(executedPlan.exists(plan => plan.isInstanceOf[ProjectExecTransformer]))
          }
      }

      // Validation: BroadHashJoin, Filter, Project
      super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
      runQueryAndCompare(
        "select type1.struct.struct_1 from type1," +
          " type2 where type1.struct.struct_1 = type2.struct.struct_1") { _ => }

      // Validation: ShuffledHashJoin, Filter, Project
      super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      runQueryAndCompare(
        "select type1.struct.struct_1 from type1," +
          " type2 where type1.struct.struct_1 = type2.struct.struct_1") { _ => }
    }
  }

  test("Decimal type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare(
      "select int, decimal from type1 " +
        " group by grouping sets(int, decimal) sort by decimal, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare(
      "select type1.decimal from type1," +
        " type2 where type1.decimal = type2.decimal") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare(
      "select type1.decimal from type1," +
        " type2 where type1.decimal = type2.decimal") { _ => }
  }

  test("Timestamp type") {
    runQueryAndCompare("select timestamp from type1 limit 100") {
      df =>
        {
          val executedPlan = getExecutedPlan(df)
          assert(
            executedPlan.exists(
              plan => plan.find(child => child.isInstanceOf[BatchScanExecTransformer]).isDefined))
        }
    }
  }

  testWithMinSparkVersion("Fallback for TimestampNTZ type scan", "3.4") {
    withTempDir {
      dir =>
        val path = new File(dir, "ntz_data").toURI.getPath
        val inputDf =
          spark.sql("SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ) AS ts_ntz")
        inputDf.write.format("parquet").save(path)
        val df = spark.read.format("parquet").load(path)
        val executedPlan = getExecutedPlan(df)
        assert(!executedPlan.exists(plan => plan.isInstanceOf[BatchScanExecTransformer]))
        checkAnswer(df, inputDf)
    }
  }

  test("Velox Parquet Write") {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {
      withTempDir {
        dir =>
          val write_path = dir.toURI.getPath
          val data_path = getClass.getResource("/").getPath + "/data-type-validation-data/type1"
          // Velox native write doesn't support Complex type.
          val df = spark.read
            .format("parquet")
            .load(data_path)
            .drop("array")
            .drop("struct")
            .drop("map")
          df.write.mode("append").format("parquet").save(write_path)
          val parquetDf = spark.read
            .format("parquet")
            .load(write_path)
          checkAnswer(parquetDf, df)
      }
    }
  }
}
