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

import org.apache.gluten.execution.IcebergScanTransformer;
import org.apache.gluten.execution.IcebergWriteExec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ReusedSubqueryExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase;
import org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec;
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import scala.Option;
import scala.collection.Iterator;

/** Observes the original upstream tests without replacing their base classes or assertions. */
public class IcebergExecutionListener extends SparkListener
    implements TestExecutionListener,
        BeforeTestExecutionCallback,
        AfterTestExecutionCallback,
        InvocationInterceptor {
  private static final AtomicReference<Coverage> ACTIVE = new AtomicReference<>();
  private static final Set<Path> REPORTS = new HashSet<>();
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final IcebergQueryTestFilter METHODS = new IcebergQueryTestFilter();
  private static String initializationError;
  private TestPlan testPlan;

  @Override
  public void testPlanExecutionStarted(TestPlan plan) {
    testPlan = plan;
  }

  @Override
  public void executionStarted(TestIdentifier test) {
    if (testClass(test).startsWith("org.apache.iceberg.")
        && test.getSource().orElse(null) instanceof MethodSource) {
      // Iceberg's parameter factories use ThreadLocalRandom. Reset before each template is
      // expanded, independently of test order, JVM startup and query execution in earlier tests.
      try {
        seedParameters(test.getUniqueId());
      } catch (ReflectiveOperationException | RuntimeException e) {
        initializationError = "Cannot stabilize upstream parameters: " + e;
      }
    }
    if (!test.isTest() || !testClass(test).startsWith("org.apache.iceberg.")) {
      return;
    }
    Coverage coverage = new Coverage();
    coverage.error = initializationError;
    if (test.getSource().orElse(null) instanceof MethodSource) {
      try {
        Class<?> concrete = Class.forName(testClass(test), false, getClass().getClassLoader());
        coverage.expectsException =
            METHODS.expectsException(
                concrete, ((MethodSource) test.getSource().get()).getJavaMethod());
      } catch (ClassNotFoundException e) {
        // Synthetic descriptors are used by the observer's own tests.
        coverage.expectsException = false;
      }
    }
    // Spark delivers SQL events asynchronously. Exclude @BeforeAll work and flush each test's
    // events before handing the fork to the next test. Jupiter parallel execution is disabled.
    drain(coverage);
    if (ACTIVE.getAndSet(coverage) != null) {
      coverage.error = "Overlapping tests: execution coverage cannot be attributed safely";
    }
  }

  static void seedParameters(String testId) throws ReflectiveOperationException {
    ThreadLocalRandom.current();
    Field seed = Thread.class.getDeclaredField("threadLocalRandomSeed");
    seed.setAccessible(true);
    seed.setLong(Thread.currentThread(), 0x9e3779b97f4a7c15L ^ testId.hashCode());
  }

  @Override
  public void interceptBeforeAllMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> method,
      ExtensionContext context)
      throws Throwable {
    seedFixture(method, context);
    invocation.proceed();
  }

  @Override
  public void interceptBeforeEachMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> method,
      ExtensionContext context)
      throws Throwable {
    seedFixture(method, context);
    invocation.proceed();
  }

  private static void seedFixture(
      ReflectiveInvocationContext<Method> method, ExtensionContext context)
      throws ReflectiveOperationException {
    if (context.getRequiredTestClass().getName().startsWith("org.apache.iceberg.")) {
      seedParameters(context.getUniqueId() + "/" + method.getExecutable().toGenericString());
    }
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    if (context.getRequiredTestClass().getName().startsWith("org.apache.iceberg.")) {
      Coverage coverage = ACTIVE.get();
      try {
        for (Class<?> type = context.getRequiredTestClass();
            type != null;
            type = type.getSuperclass()) {
          for (Field field : type.getDeclaredFields()) {
            for (Annotation annotation : field.getAnnotations()) {
              if (annotation.annotationType().getName().equals("org.apache.iceberg.Parameter")) {
                field.setAccessible(true);
                Object value = field.get(context.getRequiredTestInstance());
                coverage.parameters.put(
                    field.getName(),
                    value instanceof Map
                        ? new TreeMap<>((Map<?, ?>) value).toString()
                        : String.valueOf(value));
              }
            }
          }
        }
        Option<SparkSession> session = SparkSession.getActiveSession();
        if (session.isDefined()) {
          for (String key :
              List.of(
                  "spark.sql.adaptive.enabled",
                  "spark.sql.caseSensitive",
                  "spark.sql.ansi.enabled",
                  "spark.sql.session.timeZone",
                  "spark.sql.shuffle.partitions",
                  "spark.sql.join.preferSortMergeJoin")) {
            coverage.configuration.put(key, session.get().conf().get(key));
          }
        }
      } catch (ReflectiveOperationException | RuntimeException e) {
        coverage.error = "Cannot record upstream parameters: " + e;
      }
      beginBody();
    }
  }

  @Override
  public void afterTestExecution(ExtensionContext context) {
    if (context.getRequiredTestClass().getName().startsWith("org.apache.iceberg.")) {
      ACTIVE.get().bodyFailed = context.getExecutionException().isPresent();
      endBody();
    }
  }

  static void beginBody() {
    Coverage coverage = ACTIVE.get();
    if (coverage == null) {
      throw new IllegalStateException("Missing Iceberg test lifecycle listener");
    }
    drain(coverage);
    coverage.bodyStarted = true;
    coverage.inBody = true;
  }

  static void endBody() {
    Coverage coverage = ACTIVE.get();
    drain(coverage);
    coverage.inBody = false;
  }

  @Override
  public void executionFinished(TestIdentifier test, TestExecutionResult result) {
    if (!test.isTest() || !testClass(test).startsWith("org.apache.iceberg.")) {
      return;
    }
    Coverage coverage = ACTIVE.get();
    if (coverage == null) {
      throw new IllegalStateException("Missing execution coverage for " + test.getUniqueId());
    }
    drain(coverage);
    ACTIVE.set(null);
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("id", test.getUniqueId());
    row.put("class", testClass(test));
    row.put("name", test.getLegacyReportingName());
    row.put("display_name", test.getDisplayName());
    row.put("parameters", coverage.parameters);
    row.put("test_configuration", coverage.configuration);
    row.put("status", coverage.status(result));
    row.put("native", coverage.nativeNodes);
    row.put("fallback", coverage.fallbackNodes);
    row.put("unverified", coverage.unverifiedNodes);
    row.put("native_metrics", coverage.nativeMetrics);
    row.put("body_started", coverage.bodyStarted);
    row.put("failed_queries", coverage.failedQueries);
    row.put("expected_exception", coverage.expectsException);
    row.put(
        "failure_phase",
        !coverage.bodyStarted ? "setup" : coverage.bodyFailed ? "test" : "cleanup");
    row.put("coverage_error", coverage.error);
    write(testClass(test), row);
  }

  private String testClass(TestIdentifier test) {
    TestIdentifier current = test;
    String methodClass = "";
    while (current != null) {
      TestSource source = current.getSource().orElse(null);
      if (source instanceof MethodSource) {
        methodClass = ((MethodSource) source).getClassName();
      }
      if (source instanceof ClassSource) {
        return ((ClassSource) source).getClassName();
      }
      current = testPlan == null ? null : testPlan.getParent(current).orElse(null);
    }
    return methodClass;
  }

  private static void drain(Coverage coverage) {
    Option<SparkContext> context = SparkContext$.MODULE$.getActive();
    if (context.isDefined()) {
      try {
        context.get().listenerBus().waitUntilEmpty(30000);
      } catch (Exception e) {
        coverage.error = e.toString();
      }
    }
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    Coverage coverage = ACTIVE.get();
    if (coverage == null || !coverage.inBody || !(event instanceof SparkListenerSQLExecutionEnd)) {
      return;
    }
    SparkListenerSQLExecutionEnd end = (SparkListenerSQLExecutionEnd) event;
    // A query that throws an expected exception is not evidence of successful native execution.
    if (end.executionFailure().isDefined()) {
      coverage.failedQueries++;
      return;
    }
    if (end.qe() == null) {
      coverage.error = "SQL execution ended without a query execution plan";
      return;
    }
    try {
      coverage.observe(
          end.qe().executedPlan(), Collections.newSetFromMap(new IdentityHashMap<>()), false);
    } catch (Exception | LinkageError e) {
      coverage.error = e.toString();
    }
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd event) {
    Coverage coverage = ACTIVE.get();
    if (coverage == null || !coverage.inBody || !event.taskInfo().successful()) {
      return;
    }
    Iterator<AccumulableInfo> metrics = event.taskInfo().accumulables().iterator();
    while (metrics.hasNext()) {
      AccumulableInfo metric = metrics.next();
      if (metric.update().isDefined() && metric.update().get() instanceof Number) {
        long update = ((Number) metric.update().get()).longValue();
        if (update > 0) {
          coverage.taskMetrics.merge(metric.id(), update, Long::sum);
        }
      }
    }
  }

  private static synchronized void write(String className, Map<String, Object> row) {
    Path file =
        Paths.get(System.getProperty("gluten.iceberg.coverage.dir"))
            .resolve("coverage-" + className + ".jsonl");
    try {
      Files.createDirectories(file.getParent());
      if (REPORTS.add(file)) {
        Files.write(file, new byte[0]);
      }
      Files.write(
          file,
          (JSON.writeValueAsString(row) + "\n").getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      // The summary also rejects successful JUnit cases with missing coverage records.
      throw new IllegalStateException("Cannot write Iceberg execution coverage", e);
    }
  }

  private static boolean isIceberg(Object object) {
    if (object instanceof MicroBatchWrite) {
      object = ((MicroBatchWrite) object).writeSupport();
    }
    return object != null && object.getClass().getName().startsWith("org.apache.iceberg.");
  }

  private static class Coverage {
    private final Set<String> nativeNodes = new TreeSet<>();
    private final Set<String> fallbackNodes = new TreeSet<>();
    private final Set<String> unverifiedNodes = new TreeSet<>();
    private final Map<Long, Long> taskMetrics = new HashMap<>();
    private final List<Map<String, Object>> nativeMetrics = new ArrayList<>();
    private volatile boolean inBody;
    private boolean bodyStarted;
    private boolean bodyFailed;
    private final Map<String, String> parameters = new TreeMap<>();
    private final Map<String, String> configuration = new TreeMap<>();
    private int failedQueries;
    private boolean expectsException;
    private volatile String error;

    private void observe(SparkPlan plan, Set<SparkPlan> seen, boolean cached) {
      if (!seen.add(plan)) {
        return;
      }
      // Only inspect the executed AQE plan, not the original, untransformed plan.
      if (plan instanceof AdaptiveSparkPlanExec) {
        observe(((AdaptiveSparkPlanExec) plan).executedPlan(), seen, cached);
        return;
      }
      if (plan instanceof QueryStageExec) {
        observe(((QueryStageExec) plan).plan(), seen, cached);
        return;
      }
      if (plan instanceof ReusedExchangeExec) {
        observe(((ReusedExchangeExec) plan).child(), seen, cached);
        return;
      }
      if (plan instanceof ReusedSubqueryExec) {
        observe(((ReusedSubqueryExec) plan).child(), seen, cached);
        return;
      }
      if (plan instanceof InMemoryTableScanExec) {
        observe(((InMemoryTableScanExec) plan).relation().cachedPlan(), seen, true);
      }
      if (plan instanceof IcebergScanTransformer || plan instanceof IcebergWriteExec) {
        Map<String, Long> executed = new LinkedHashMap<>();
        Set<String> executionMetrics =
            Set.of(
                "rawInputRows",
                "rawInputBytes",
                "numOutputRows",
                "processedSplits",
                "wallNanos",
                "numWrittenBytes",
                "numWrittenFiles",
                "writeWallNs");
        scala.collection.Map<String, SQLMetric> metrics =
            plan instanceof IcebergWriteExec
                ? ((IcebergWriteExec) plan).customMetrics()
                : plan.metrics();
        Iterator<scala.Tuple2<String, SQLMetric>> values = metrics.iterator();
        while (values.hasNext()) {
          scala.Tuple2<String, SQLMetric> value = values.next();
          if (executionMetrics.contains(value._1()) && taskMetrics.containsKey(value._2().id())) {
            executed.put(value._1(), taskMetrics.get(value._2().id()));
          }
        }
        String node = plan.getClass().getSimpleName();
        if (executed.isEmpty()) {
          unverifiedNodes.add(node);
        } else {
          nativeNodes.add(node);
          nativeMetrics.add(Map.of("node", node, "metrics", executed));
        }
      } else if (plan instanceof DataSourceV2ScanExecBase
          && isIceberg(((DataSourceV2ScanExecBase) plan).scan())) {
        fallback(
            plan,
            cached,
            plan.getClass().getSimpleName()
                + ":"
                + ((DataSourceV2ScanExecBase) plan).scan().getClass().getSimpleName());
      } else if (plan instanceof V2ExistingTableWriteExec
          && isIceberg(((V2ExistingTableWriteExec) plan).write())) {
        fallback(
            plan,
            cached,
            plan.getClass().getSimpleName()
                + ":"
                + ((V2ExistingTableWriteExec) plan).write().getClass().getSimpleName());
      } else if (plan instanceof WriteToDataSourceV2Exec
          && isIceberg(((WriteToDataSourceV2Exec) plan).batchWrite())) {
        fallback(plan, cached, plan.getClass().getSimpleName());
      }
      Iterator<SparkPlan> children = plan.children().iterator();
      while (children.hasNext()) {
        observe(children.next(), seen, cached);
      }
      // Includes command plans, reused exchanges and subqueries, which can be hidden from children.
      Iterator<?> inner = plan.innerChildren().iterator();
      while (inner.hasNext()) {
        Object child = inner.next();
        if (child instanceof SparkPlan) {
          observe((SparkPlan) child, seen, cached);
        }
      }
    }

    private void fallback(SparkPlan plan, boolean cached, String node) {
      if (cached) {
        // A cache retains its original Spark scan plan after materialization. Only fresh
        // task updates establish that this body actually executed the cached Iceberg scan.
        Iterator<SQLMetric> metrics = plan.metrics().valuesIterator();
        while (metrics.hasNext()) {
          if (taskMetrics.containsKey(metrics.next().id())) {
            fallbackNodes.add(node);
            return;
          }
        }
        unverifiedNodes.add(node);
      } else {
        fallbackNodes.add(node);
      }
    }

    private String status(TestExecutionResult result) {
      if (error != null) {
        return "COVERAGE_ERROR";
      }
      if (result.getStatus() == TestExecutionResult.Status.FAILED) {
        return "FAILED";
      }
      if (result.getStatus() == TestExecutionResult.Status.ABORTED) {
        return "SKIPPED";
      }
      if (!bodyStarted) {
        error = "JUnit test-body extension did not run";
        return "COVERAGE_ERROR";
      }
      if (!fallbackNodes.isEmpty()) {
        return "PASSED_FALLBACK";
      }
      if (!unverifiedNodes.isEmpty()
          || ((failedQueries > 0 || expectsException) && !nativeNodes.isEmpty())) {
        return "PASSED_UNVERIFIED";
      }
      return nativeNodes.isEmpty() ? "NO_ICEBERG_EXECUTION" : "PASSED_NATIVE";
    }
  }
}
