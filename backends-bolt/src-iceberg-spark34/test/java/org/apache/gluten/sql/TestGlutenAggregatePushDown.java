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
package org.apache.gluten.sql;

import org.apache.gluten.TestConfUtil;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.sql.TestAggregatePushDown;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;

import java.util.Map;

public class TestGlutenAggregatePushDown extends TestAggregatePushDown {
  public TestGlutenAggregatePushDown(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.iceberg.aggregate_pushdown", "true")
            .config(TestConfUtil.GLUTEN_CONF)
            .enableHiveSupport()
            .getOrCreate();

    SparkTestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }
}
