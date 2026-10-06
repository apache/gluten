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
package org.apache.gluten.execution;

import org.apache.gluten.config.GlutenConfig;

import org.apache.commons.io.FileUtils;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTPCHStoragePartitionedJoins extends SparkTestBaseWithCatalog {
  protected String rootPath = this.getClass().getResource("/").getPath();

  protected String tpchQueries =
      rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries";

  // open file cost and split size are set as 16 MB to produce a split per file
  private static final Map<String, String> TABLE_PROPERTIES =
      ImmutableMap.of(
          TableProperties.SPLIT_SIZE, "16777216", TableProperties.SPLIT_OPEN_FILE_COST, "16777216");

  // only v2 bucketing and preserve data grouping properties have to be enabled to trigger SPJ
  // other properties are only to simplify testing and validation
  private static final Map<String, String> ENABLED_SPJ_SQL_CONF =
      ImmutableMap.of(
          SQLConf.V2_BUCKETING_ENABLED().key(),
          "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED().key(),
          "true",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
          "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
          "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
          "-1",
          SparkSQLProperties.PRESERVE_DATA_GROUPING,
          "true",
          SQLConf.V2_BUCKETING_PARTIALLY_CLUSTERED_DISTRIBUTION_ENABLED().key(),
          "true");
  protected static String PARQUET_TABLE_PREFIX = "p_";
  protected static List<String> tableNames =
      ImmutableList.of(
          "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region");

  // If we test all the catalog, we need to create the table in that catalog,
  // we don't need to test the catalog, so only test the testhadoop catalog
  @Before
  public void createTPCHNotNullTables() {
    final String tableDir = this.getClass().getResource("/tpch-data-parquet").getFile();
    tableNames.forEach(
        table -> {
          String tablePath = new File(tableDir, table).getAbsolutePath();
          Dataset<Row> tableDF = spark.read().format("parquet").load(tablePath);
          tableDF.createOrReplaceTempView(PARQUET_TABLE_PREFIX + table);
        });

    sql(
        createIcebergTable(
            "part",
            "`p_partkey`     INT,\n"
                + "  `p_name`        string,\n"
                + "  `p_mfgr`        string,\n"
                + "  `p_brand`       string,\n"
                + "  `p_type`        string,\n"
                + "  `p_size`        INT,\n"
                + "  `p_container`   string,\n"
                + "  `p_retailprice` DECIMAL(15,2) ,\n"
                + "  `p_comment`     string ",
            null));
    sql(
        createIcebergTable(
            "nation",
            "`n_nationkey`  INT,\n"
                + "  `n_name`       CHAR(25),\n"
                + "  `n_regionkey`  INT,\n"
                + "  `n_comment`    VARCHAR(152)"));
    sql(
        createIcebergTable(
            "region",
            "`r_regionkey`  INT,\n"
                + "  `r_name`       CHAR(25),\n"
                + "  `r_comment`    VARCHAR(152)"));
    sql(
        createIcebergTable(
            "supplier",
            "`s_suppkey`     INT,\n"
                + "  `s_name`        CHAR(25),\n"
                + "  `s_address`     VARCHAR(40),\n"
                + "  `s_nationkey`   INT,\n"
                + "  `s_phone`       CHAR(15),\n"
                + "  `s_acctbal`     DECIMAL(15,2),\n"
                + "  `s_comment`     VARCHAR(101)"));
    sql(
        createIcebergTable(
            "customer",
            "`c_custkey`     INT,\n"
                + "  `c_name`        string,\n"
                + "  `c_address`     string,\n"
                + "  `c_nationkey`   INT,\n"
                + "  `c_phone`       string,\n"
                + "  `c_acctbal`     DECIMAL(15,2),\n"
                + "  `c_mktsegment`  string,\n"
                + "  `c_comment`     string",
            "bucket(16, c_custkey)"));
    sql(
        createIcebergTable(
            "partsupp",
            "`ps_partkey`     INT,\n"
                + "  `ps_suppkey`     INT,\n"
                + "  `ps_availqty`    INT,\n"
                + "  `ps_supplycost`  DECIMAL(15,2),\n"
                + "  `ps_comment`     VARCHAR(199)"));
    sql(
        createIcebergTable(
            "orders",
            "`o_orderkey`       INT,\n"
                + "  `o_custkey`        INT,\n"
                + "  `o_orderstatus`    string,\n"
                + "  `o_totalprice`     DECIMAL(15,2),\n"
                + "  `o_orderdate`      DATE,\n"
                + "  `o_orderpriority`  string,\n"
                + "  `o_clerk`          string,\n"
                + "  `o_shippriority`   INT,\n"
                + "  `o_comment`        string",
            "bucket(16, o_custkey)"));

    sql(
        createIcebergTable(
            "lineitem",
            "`l_orderkey`    INT,\n"
                + "  `l_partkey`     INT,\n"
                + "  `l_suppkey`     INT,\n"
                + "  `l_linenumber`  INT,\n"
                + "  `l_quantity`    DECIMAL(15,2),\n"
                + "  `l_extendedprice`  DECIMAL(15,2),\n"
                + "  `l_discount`    DECIMAL(15,2),\n"
                + "  `l_tax`         DECIMAL(15,2),\n"
                + "  `l_returnflag`  string,\n"
                + "  `l_linestatus`  string,\n"
                + "  `l_shipdate`    DATE,\n"
                + "  `l_commitdate`  DATE,\n"
                + "  `l_receiptdate` DATE,\n"
                + "  `l_shipinstruct` string,\n"
                + "  `l_shipmode`    string,\n"
                + "  `l_comment`     string",
            null));

    String insertStmt = "INSERT INTO %s select * from %s%s";
    tableNames.forEach(
        table -> sql(String.format(insertStmt, tableName(table), PARQUET_TABLE_PREFIX, table)));
  }

  @After
  public void dropTPCHNotNullTables() {
    tableNames.forEach(
        table -> {
          sql("DROP TABLE IF EXISTS " + tableName(table));
          sql("DROP VIEW IF EXISTS " + PARQUET_TABLE_PREFIX + table);
        });
  }

  private String createIcebergTable(String name, String columns) {
    return createIcebergTable(name, columns, null);
  }

  private String createIcebergTable(String name, String columns, String transform) {
    // create TPCH iceberg table
    String createTableStmt =
        "CREATE TABLE %s (%s)" + "USING iceberg " + "PARTITIONED BY (%s)" + "TBLPROPERTIES (%s)";
    String createUnpartitionTableStmt =
        "CREATE TABLE %s (%s)" + "USING iceberg " + "TBLPROPERTIES (%s)";
    if (transform != null) {
      return String.format(
          createTableStmt,
          tableName(name),
          columns,
          transform,
          tablePropsAsString(TABLE_PROPERTIES));
    } else {
      return String.format(
          createUnpartitionTableStmt,
          tableName(name),
          columns,
          tablePropsAsString(TABLE_PROPERTIES));
    }
  }

  protected String tpchSQL(int queryNum) {
    try {
      return FileUtils.readFileToString(new File(tpchQueries + "/q" + queryNum + ".sql"), "UTF-8");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testTPCH() {
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
    spark.conf().set("spark.sql.catalog." + catalogName + ".default-namespace", "default");
    sql("use namespace default");
    withSQLConf(
        ENABLED_SPJ_SQL_CONF,
        () -> {
          for (int i = 1; i <= 22; i++) {
            List<Row> rows = spark.sql(tpchSQL(i)).collectAsList();
            AtomicReference<List<Row>> rowsSpark = new AtomicReference<>();
            int finalI = i;
            withSQLConf(
                ImmutableMap.of(GlutenConfig.GLUTEN_ENABLED().key(), "false"),
                () -> rowsSpark.set(spark.sql(tpchSQL(finalI)).collectAsList()));
            assertThat(rows).containsExactlyInAnyOrderElementsOf(Iterables.concat(rowsSpark.get()));
          }
        });
  }
}
