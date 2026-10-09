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
package org.apache.gluten.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.engine.support.descriptor.AbstractTestDescriptor;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestIdentifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergExecutionListenerTest {
  @TempDir static Path directory;
  private static SparkSession spark;
  private static String previousDirectory;

  @BeforeAll
  static void startSpark() {
    previousDirectory = System.getProperty("gluten.iceberg.coverage.dir");
    System.setProperty("gluten.iceberg.coverage.dir", directory.toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("Iceberg coverage checks")
            .config("spark.sql.catalog.coverage", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.coverage.type", "hadoop")
            .config(
                "spark.sql.catalog.coverage.warehouse", directory.resolve("warehouse").toString())
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
    spark.sql("CREATE TABLE coverage.default.data (id BIGINT) USING iceberg");
    spark.sql("INSERT INTO coverage.default.data VALUES (1), (2)");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
    if (previousDirectory == null) {
      System.clearProperty("gluten.iceberg.coverage.dir");
    } else {
      System.setProperty("gluten.iceberg.coverage.dir", previousDirectory);
    }
  }

  @Test
  void nativeReadAndWrite() throws Exception {
    JsonNode result =
        observe(
            "nativeReadAndWrite",
            TestExecutionResult.successful(),
            () -> {
              spark.sql("INSERT INTO coverage.default.data VALUES (3)");
              spark.sql("SELECT id + 1 FROM coverage.default.data").collectAsList();
            });
    assertEquals("PASSED_NATIVE", result.path("status").asText());
    assertTrue(result.path("native").toString().contains("IcebergScanTransformer"));
    assertTrue(result.path("native").toString().contains("VeloxIcebergAppendDataExec"));
    assertTrue(result.path("native_metrics").size() >= 2, result.toString());
  }

  @Test
  void sparkFallbackDoesNotCountOtherNativeOperators() throws Exception {
    spark.conf().set("spark.gluten.sql.columnar.iceberg.enableNativeRead", false);
    spark.conf().set("spark.gluten.sql.columnar.iceberg.enableNativeWrite", false);
    try {
      JsonNode result =
          observe(
              "fallback",
              TestExecutionResult.successful(),
              () -> {
                spark.sql("INSERT INTO coverage.default.data VALUES (4)");
                spark.sql("SELECT id + 1 FROM coverage.default.data").collectAsList();
              });
      assertEquals("PASSED_FALLBACK", result.path("status").asText());
      assertTrue(result.path("native").isEmpty());
      assertTrue(result.path("fallback").toString().contains("BatchScanExec"));
      assertTrue(result.path("fallback").toString().contains("AppendDataExec"));
    } finally {
      spark.conf().unset("spark.gluten.sql.columnar.iceberg.enableNativeRead");
      spark.conf().unset("spark.gluten.sql.columnar.iceberg.enableNativeWrite");
    }
  }

  @Test
  void mixedExecutionCountsAsFallbackAndDoesNotLeakToNextTest() throws Exception {
    spark.conf().set("spark.gluten.sql.columnar.iceberg.enableNativeWrite", false);
    try {
      JsonNode result =
          observe(
              "mixed",
              TestExecutionResult.successful(),
              () -> {
                spark.sql("INSERT INTO coverage.default.data VALUES (5)");
                spark.sql("SELECT id FROM coverage.default.data").collectAsList();
              });
      assertEquals("PASSED_FALLBACK", result.path("status").asText());
      assertTrue(result.path("native").toString().contains("IcebergScanTransformer"));
      assertTrue(result.path("fallback").toString().contains("AppendDataExec"));
    } finally {
      spark.conf().unset("spark.gluten.sql.columnar.iceberg.enableNativeWrite");
    }
    JsonNode next =
        observe(
            "next",
            TestExecutionResult.successful(),
            () -> spark.sql("SELECT id FROM coverage.default.data").collectAsList());
    assertEquals("PASSED_NATIVE", next.path("status").asText());
  }

  @Test
  void seesIcebergInsideAdaptiveSubquery() throws Exception {
    JsonNode result =
        observe(
            "subquery",
            TestExecutionResult.successful(),
            () ->
                spark
                    .sql("SELECT (SELECT sum(id + 1) FROM coverage.default.data)")
                    .collectAsList());
    assertEquals("PASSED_NATIVE", result.path("status").asText());
    assertTrue(result.path("native").toString().contains("IcebergScanTransformer"));
  }

  @Test
  void seesIcebergWhenMaterializingCachedData() throws Exception {
    for (boolean nativeRead : new boolean[] {true, false}) {
      spark.conf().set("spark.gluten.sql.columnar.iceberg.enableNativeRead", nativeRead);
      Dataset<Row> cached = spark.sql("SELECT id FROM coverage.default.data").cache();
      try {
        JsonNode result =
            observe("cache" + nativeRead, TestExecutionResult.successful(), cached::collectAsList);
        assertEquals(
            nativeRead ? "PASSED_NATIVE" : "PASSED_FALLBACK", result.path("status").asText());
      } finally {
        cached.unpersist(true);
        spark.conf().unset("spark.gluten.sql.columnar.iceberg.enableNativeRead");
      }
    }
  }

  @Test
  void alreadyMaterializedCacheDoesNotProveIcebergExecution() throws Exception {
    for (boolean nativeRead : new boolean[] {true, false}) {
      spark.conf().set("spark.gluten.sql.columnar.iceberg.enableNativeRead", nativeRead);
      Dataset<Row> cached = spark.sql("SELECT id FROM coverage.default.data").cache();
      cached.collectAsList();
      try {
        JsonNode result =
            observe(
                "warmCache" + nativeRead, TestExecutionResult.successful(), cached::collectAsList);
        assertEquals("PASSED_UNVERIFIED", result.path("status").asText(), result.toString());
        assertTrue(result.path("native").isEmpty(), result.toString());
        assertTrue(result.path("fallback").isEmpty(), result.toString());
      } finally {
        cached.unpersist(true);
        spark.conf().unset("spark.gluten.sql.columnar.iceberg.enableNativeRead");
      }
    }
  }

  @Test
  void setupAndCleanupCannotSupplyNativeCoverage() throws Exception {
    JsonNode result =
        observe(
            "fixtures",
            TestExecutionResult.successful(),
            () -> spark.sql("SELECT id FROM coverage.default.data").collectAsList(),
            () -> spark.sql("SELECT 1").collectAsList(),
            () -> spark.sql("SELECT id FROM coverage.default.data").collectAsList());
    assertEquals("NO_ICEBERG_EXECUTION", result.path("status").asText(), result.toString());
    assertTrue(result.path("native").isEmpty());
  }

  @Test
  void nativePlanWithoutExecutorWorkIsNotNativeCoverage() throws Exception {
    spark.sql("CREATE TABLE coverage.default.empty (id BIGINT) USING iceberg");
    try {
      JsonNode result =
          observe(
              "empty",
              TestExecutionResult.successful(),
              () -> spark.sql("SELECT id FROM coverage.default.empty").collectAsList());
      assertTrue(result.path("native").isEmpty(), result.toString());
      assertTrue(!result.path("status").asText().equals("PASSED_NATIVE"), result.toString());
    } finally {
      spark.sql("DROP TABLE coverage.default.empty");
    }
  }

  @Test
  void parameterSeedIsIndependentOfEarlierRandomWork() throws Exception {
    IcebergExecutionListener.seedParameters("upstream/template");
    long expected = ThreadLocalRandom.current().nextLong();
    for (int i = 0; i < 100; i++) {
      ThreadLocalRandom.current().nextLong();
    }
    IcebergExecutionListener.seedParameters("upstream/template");
    assertEquals(expected, ThreadLocalRandom.current().nextLong());
  }

  @Test
  void assertionFailureTakesPrecedenceOverNativeExecution() throws Exception {
    JsonNode result =
        observe(
            "failed",
            TestExecutionResult.failed(new AssertionError("upstream assertion failure")),
            () -> spark.sql("SELECT id FROM coverage.default.data").collectAsList());
    assertEquals("FAILED", result.path("status").asText());
    assertTrue(result.path("native").toString().contains("IcebergScanTransformer"));
  }

  @Test
  void noIcebergExecutionIsNotNativeCoverage() throws Exception {
    JsonNode result =
        observe(
            "noIceberg",
            TestExecutionResult.successful(),
            () -> spark.sql("SELECT 1").collectAsList());
    assertEquals("NO_ICEBERG_EXECUTION", result.path("status").asText());
  }

  @Test
  void abortedTestIsSkipped() throws Exception {
    JsonNode result =
        observe("skipped", TestExecutionResult.aborted(new Exception("assumption")), () -> {});
    assertEquals("SKIPPED", result.path("status").asText());
  }

  private JsonNode observe(String name, TestExecutionResult outcome, Runnable action)
      throws Exception {
    return observe(name, outcome, () -> {}, action, () -> {});
  }

  private JsonNode observe(
      String name, TestExecutionResult outcome, Runnable setup, Runnable action, Runnable cleanup)
      throws Exception {
    AbstractTestDescriptor descriptor =
        new AbstractTestDescriptor(
            UniqueId.forEngine("junit-jupiter").append("test", name),
            name,
            MethodSource.from("org.apache.iceberg.CoverageProbe", name)) {
          @Override
          public Type getType() {
            return Type.TEST;
          }
        };
    TestIdentifier test = TestIdentifier.from(descriptor);
    IcebergExecutionListener listener = new IcebergExecutionListener();
    listener.executionStarted(test);
    try {
      setup.run();
      IcebergExecutionListener.beginBody();
      action.run();
    } finally {
      IcebergExecutionListener.endBody();
      cleanup.run();
      listener.executionFinished(test, outcome);
    }
    List<String> rows =
        Files.readAllLines(directory.resolve("coverage-org.apache.iceberg.CoverageProbe.jsonl"));
    return new ObjectMapper().readTree(rows.get(rows.size() - 1));
  }
}
