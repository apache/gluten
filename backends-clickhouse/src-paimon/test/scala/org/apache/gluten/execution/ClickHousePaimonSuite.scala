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
import org.apache.spark.sql.{DataFrame, Row}

class ClickHousePaimonSuite extends GlutenClickHouseWholeStageTransformerSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$dataHome/data-paimon")
  }

  private def checkNativePaimonScan(df: DataFrame): Unit = {
    checkGlutenPlan[BatchScanExecTransformerBase](df)
    assert(df.queryExecution.executedPlan.exists {
      case stage: WholeStageTransformer =>
        stage.child.exists {
          case _: PaimonScanTransformer | _: CHPaimonScanTransformer => true
          case _ => false
        }
      case _ => false
    })
  }

  Seq("parquet", "orc").foreach { format =>
    test(s"read Paimon $format table with ClickHouse") {
      assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
      withTable("paimon_ch_table") {
        withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
          sql(s"CREATE TABLE paimon_ch_table (id INT, name STRING) USING paimon " +
            s"TBLPROPERTIES ('file.format'='$format')")
          sql("INSERT INTO paimon_ch_table VALUES (1, 'Bob'), (2, 'Alice')")
        }
        runQueryAndCompare("SELECT id, name FROM paimon_ch_table ORDER BY id") {
          checkNativePaimonScan
        }
      }
    }
  }

  test("read non-primary-key Paimon table using native SDK") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_native_read") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_native_read (id INT, name STRING) USING paimon " +
          "TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
        sql("INSERT INTO paimon_ch_native_read VALUES (1, 'Bob'), (2, 'Alice')")
      }

      runQueryAndCompare("SELECT id, name FROM paimon_ch_native_read ORDER BY id") { df =>
        checkNativePaimonScan(df)
        assert(df.queryExecution.executedPlan.exists {
          case _: CHPaimonScanTransformer => true
          case _ => false
        })
      }
    }
  }

  test("native Paimon scan preserves branch") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_branch") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_branch (id INT) USING paimon " +
          "TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
        sql("INSERT INTO paimon_ch_branch VALUES (1)")
        sql("CALL sys.create_tag('default.paimon_ch_branch', 'review_tag')")
        sql("CALL sys.create_branch('default.paimon_ch_branch', 'review_branch', 'review_tag')")
        sql("INSERT INTO paimon_ch_branch VALUES (2)")
      }

      val path = s"file://$dataHome/data-paimon/default.db/paimon_ch_branch"
      def readBranch(): DataFrame = spark.read.format("paimon")
        .option("path", path)
        .option("branch", "review_branch")
        .load()
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        assert(readBranch().collect().toSeq == Seq(Row(1)))
      }
      val df = readBranch()
      assert(df.collect().toSeq == Seq(Row(1)))
      val nativeScans = df.queryExecution.executedPlan.collect {
        case scan: CHPaimonScanTransformer => scan
      }
      assert(nativeScans.nonEmpty, df.queryExecution.executedPlan.toString())
    }
  }

  test("native Paimon scan applies a query-level read-option override") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_read_options") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_read_options (id INT) USING paimon " +
          "TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet', 'read.batch-size'='64')")
        sql("INSERT INTO paimon_ch_read_options VALUES (1), (2)")
      }

      val path = s"file://$dataHome/data-paimon/default.db/paimon_ch_read_options"
      val df = spark.read.format("paimon")
        .option("path", path)
        .option("read.batch-size", "1")
        .load()
      assert(df.collect().toSet == Set(Row(1), Row(2)))
      val nativeScans = df.queryExecution.executedPlan.collect {
        case scan: CHPaimonScanTransformer => scan
      }
      assert(nativeScans.nonEmpty, df.queryExecution.executedPlan.toString())
      nativeScans.foreach { scan =>
        assert(scan.effectiveReadOptions.get("read.batch-size").contains("1"))
      }
    }
  }

  test("incremental diff scan falls back to Paimon JVM reader") {
    withTable("paimon_ch_incremental_fallback") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_incremental_fallback " +
          "(id INT, name STRING) USING paimon " +
          "TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'file.format'='parquet')")
        sql("INSERT INTO paimon_ch_incremental_fallback VALUES (1, 'old')")
        sql("INSERT INTO paimon_ch_incremental_fallback VALUES (1, 'new')")
      }

      val path = s"file://$dataHome/data-paimon/default.db/paimon_ch_incremental_fallback"
      val df = spark.read.format("paimon")
        .option("path", path)
        .option("incremental-between", "1,2")
        .option("incremental-between-scan-mode", "diff")
        .load()
      assert(df.collect().toSeq == Seq(Row(1, "new")))
      assert(!df.queryExecution.executedPlan.exists {
        case _: CHPaimonScanTransformer => true
        case _ => false
      })
    }
  }

  test("sum one over append-only Paimon rows in ClickHouse") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_native_sum_one") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_native_sum_one (id STRING, p STRING) USING paimon " +
          "PARTITIONED BY (p) TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
        sql("INSERT INTO paimon_ch_native_sum_one VALUES ('1', 'view'), " +
          "(NULL, 'view'), ('3', 'click')")
      }

      runQueryAndCompare(
        "SELECT sum(1) FROM paimon_ch_native_sum_one WHERE p = 'view'") { df =>
        assert(df.collect().toSeq == Seq(Row(2L)))
        assert(df.queryExecution.executedPlan.exists {
          case _: CHPaimonScanTransformer => true
          case _ => false
        })
      }
      assert(sql("SELECT sum(1) FROM paimon_ch_native_sum_one WHERE p = 'missing'")
        .collect().toSeq == Seq(Row(null)))
    }
  }

  test("convert Paimon local count result to a CH batch") {
    withTable("paimon_ch_local_count") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_local_count (id STRING, p STRING) USING paimon " +
          "PARTITIONED BY (p) TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
        sql("INSERT INTO paimon_ch_local_count VALUES ('1', 'view'), ('2', 'view')")
      }

      runQueryAndCompare(
        "SELECT count(1) FROM paimon_ch_local_count WHERE p = 'view'") { df =>
        assert(df.collect().toSeq == Seq(Row(2L)))
        assert(df.queryExecution.executedPlan.exists {
          case _: CHPaimonCountTransformer => true
          case _ => false
        })
      }
    }
  }

  test("read partition columns from Paimon table with ClickHouse") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_partitioned") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_partitioned (id INT, p STRING) " +
          "USING paimon PARTITIONED BY (p)")
        sql("INSERT INTO paimon_ch_partitioned VALUES (1, 'a'), (2, 'b')")
      }
      runQueryAndCompare("SELECT id, p FROM paimon_ch_partitioned ORDER BY id") {
        checkNativePaimonScan
      }
    }
  }

  test("offload primary key table by default") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_native_pk") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_native_pk (id INT, name STRING) USING paimon " +
          "TBLPROPERTIES ('primary-key'='id', 'bucket'='1', " +
          "'file.format'='parquet', 'write-only'='true')")
        sql("INSERT INTO paimon_ch_native_pk VALUES (1, 'old')")
        sql("INSERT INTO paimon_ch_native_pk VALUES (1, 'new')")
      }

      runQueryAndCompare("SELECT id, name FROM paimon_ch_native_pk") { df =>
        assert(df.collect().toSeq == Seq(Row(1, "new")))
        checkNativePaimonScan(df)
        assert(df.queryExecution.executedPlan.exists {
          case _: CHPaimonScanTransformer => true
          case _ => false
        })
      }
    }
  }

  test("count non-null primary key rows with native Paimon reader") {
    assume(sys.env.get("GLUTEN_TEST_NATIVE_PAIMON").contains("1"))
    withTable("paimon_ch_native_pk_count") {
      withSQLConf(GlutenConfig.GLUTEN_ENABLED.key -> "false") {
        sql("CREATE TABLE paimon_ch_native_pk_count (id INT NOT NULL, p STRING NOT NULL, " +
          "name STRING) USING paimon PARTITIONED BY (p) " +
          "TBLPROPERTIES ('primary-key'='p,id', 'bucket'='1', " +
          "'file.format'='parquet', 'write-only'='true')")
        sql("INSERT INTO paimon_ch_native_pk_count VALUES (1, 'view', 'old')")
        sql("INSERT INTO paimon_ch_native_pk_count VALUES (1, 'view', 'new')")
        sql("INSERT INTO paimon_ch_native_pk_count VALUES (2, 'click', 'other')")
      }

      Seq("COUNT(id)", "COUNT(*)").foreach { count =>
        runQueryAndCompare(
          s"SELECT $count FROM paimon_ch_native_pk_count WHERE p = 'view'") { df =>
          assert(df.collect().toSeq == Seq(Row(1L)))
          checkNativePaimonScan(df)
        }
      }
    }
  }

}
