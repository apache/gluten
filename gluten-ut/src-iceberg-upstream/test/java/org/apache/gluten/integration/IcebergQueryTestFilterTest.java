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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergQueryTestFilterTest {
  private final IcebergQueryTestFilter filter = new IcebergQueryTestFilter();

  @Test
  void keepsQueriesWritesAndLambdaHelpers() throws Exception {
    for (String method :
        new String[] {"query", "write", "lambda", "concatenatedQuery", "formatShortcut"}) {
      assertTrue(selected(Examples.class, method), method);
    }
  }

  @Test
  void rejectsCatalogOnlyAndDirectJavaOperations() throws Exception {
    assertFalse(selected(Examples.class, "catalogOnly"));
    assertFalse(selected(Examples.class, "javaOnly"));
    assertFalse(selected(Examples.class, "parserOnly"));
  }

  @Test
  void resolvesInheritedTestsAgainstConcreteQueryImplementation() throws Exception {
    assertTrue(selected(QuerySubclass.class, "inheritedTest"));
    assertFalse(selected(JavaSubclass.class, "inheritedTest"));
  }

  @Test
  void keepsOriginalUpstreamQueryMethodsIncludingInheritedScans() throws Exception {
    assertTrue(upstream("org.apache.iceberg.spark.sql.TestSelect", "testSelect"));
    assertTrue(upstream("org.apache.iceberg.spark.source.TestAvroScan", "testArray"));
    assertTrue(upstream("org.apache.iceberg.spark.source.TestParquetScan", "testArray"));
    assertTrue(
        upstream(
            "org.apache.iceberg.spark.actions.TestComputeTableStatsAction",
            "testComputeTableStatsWithNestedSchema"));
    assertTrue(
        upstream(
            "org.apache.iceberg.spark.extensions.TestCopyOnWriteDelete",
            "testDeleteWithExistSubquery"));
  }

  @Test
  void rejectsOriginalUpstreamNonQueryMethods() throws Exception {
    assertFalse(upstream("org.apache.iceberg.spark.sql.TestAlterTable", "testAddColumn"), "DDL");
    assertFalse(
        upstream(
            "org.apache.iceberg.TestTableSerialization", "testSerializableTableKryoSerialization"),
        "Serialization");
    assertFalse(
        upstream("org.apache.iceberg.spark.data.TestSparkAvroReader", "testArray"), "Java reader");
  }

  @Test
  void identifiesExpectedErrorsWithoutTreatingThemAsNativeProof() throws Exception {
    assertTrue(filter.expectsException(Examples.class, Examples.class.getMethod("expectedError")));
    assertFalse(filter.expectsException(Examples.class, Examples.class.getMethod("query")));
  }

  @Test
  void discoversAllUpstreamSparkTestsWithTheirHelpers() throws Exception {
    List<DiscoverySelector> selectors = new ArrayList<>();
    for (String representative :
        new String[] {
          "org.apache.iceberg.spark.sql.TestSelect",
          "org.apache.iceberg.spark.extensions.TestCopyOnWriteDelete"
        }) {
      Class<?> type = Class.forName(representative, false, getClass().getClassLoader());
      try (JarFile jar =
          new JarFile(new File(type.getProtectionDomain().getCodeSource().getLocation().toURI()))) {
        jar.stream()
            .map(entry -> entry.getName())
            .filter(name -> name.startsWith("org/apache/iceberg/") && name.endsWith(".class"))
            .forEach(
                name ->
                    selectors.add(
                        DiscoverySelectors.selectClass(
                            name.substring(0, name.length() - ".class".length())
                                .replace('/', '.'))));
      }
    }
    Set<String> expected = discoverMethods(selectors, 0, 1);
    for (int shards : new int[] {3, 5, 12}) {
      Set<String> actual = new HashSet<>();
      for (int shard = 0; shard < shards; shard++) {
        Set<String> methods = discoverMethods(selectors, shard, shards);
        assertFalse(methods.isEmpty(), "Empty shard " + shard);
        for (String method : methods) {
          assertTrue(actual.add(method), "Test selected by more than one shard: " + method);
        }
      }
      assertEquals(expected, actual);
    }
  }

  @Test
  void validatesShardConfigurationAndMatchesReportMerger() {
    assertThrows(IllegalArgumentException.class, () -> new IcebergQueryTestFilter(0, 0));
    assertThrows(IllegalArgumentException.class, () -> new IcebergQueryTestFilter(-1, 12));
    assertThrows(IllegalArgumentException.class, () -> new IcebergQueryTestFilter(12, 12));
    assertEquals(
        0,
        IcebergQueryTestFilter.shardFor(
            "org.apache.iceberg.spark.sql.TestSelect", "testSelect", 1));
    assertEquals(
        5,
        IcebergQueryTestFilter.shardFor(
            "org.apache.iceberg.spark.sql.TestSelect", "testSelect", 12));
    for (int shards : new int[] {3, 5}) {
      assertEquals(
          IcebergQueryTestFilter.shardFor(
              "org.apache.iceberg.spark.sql.TestSelect", "testSelect", shards),
          IcebergQueryTestFilter.shardFor(
              "org.apache.iceberg.spark.sql.TestSelect", "anotherMethod", shards));
    }
  }

  private Set<String> discoverMethods(List<DiscoverySelector> selectors, int shard, int shards) {
    TestPlan plan =
        LauncherFactory.create(
                LauncherConfig.builder().enablePostDiscoveryFilterAutoRegistration(false).build())
            .discover(
                LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectors)
                    .filters(new IcebergQueryTestFilter(shard, shards))
                    .build());
    return plan.getRoots().stream()
        .flatMap(root -> plan.getDescendants(root).stream())
        .filter(test -> test.getSource().orElse(null) instanceof MethodSource)
        .map(TestIdentifier::getUniqueId)
        .collect(Collectors.toSet());
  }

  private boolean upstream(String className, String name) throws Exception {
    return selected(Class.forName(className, false, getClass().getClassLoader()), name);
  }

  private boolean selected(Class<?> type, String name) throws Exception {
    Method method = type.getMethod(name);
    return filter.applicable(type, method);
  }

  public static class Examples {
    private SparkSession spark;
    private Dataset<Row> data;

    private void sql(String query) {
      spark.sql(query).collectAsList();
    }

    public void query() {
      sql("SELECT * FROM data");
    }

    public void expectedError() {
      org.junit.jupiter.api.Assertions.assertThrows(
          IllegalArgumentException.class, () -> spark.sql("SELECT * FROM missing"));
    }

    public void write() throws Exception {
      data.writeTo("data").append();
    }

    public void formatShortcut() {
      data.write().parquet("output");
    }

    public void lambda() {
      Runnable action = () -> data.collectAsList();
      action.run();
    }

    public void concatenatedQuery() {
      sql("SELECT * FROM " + data.toString());
    }

    public void catalogOnly() {
      sql("ALTER TABLE data ADD COLUMN id INT");
    }

    public void javaOnly() {
      Integer.parseInt("1");
    }

    public void parserOnly() throws Exception {
      spark.sessionState().sqlParser().parsePlan("CALL catalog.system.rewrite_data_files('data')");
    }
  }

  public abstract static class Base {
    public void inheritedTest() {
      read();
    }

    protected abstract void read();
  }

  public static class QuerySubclass extends Base {
    @Override
    protected void read() {
      Dataset<Row> data = null;
      data.collectAsList();
    }
  }

  public static class JavaSubclass extends Base {
    @Override
    protected void read() {
      Integer.parseInt("1");
    }
  }
}
