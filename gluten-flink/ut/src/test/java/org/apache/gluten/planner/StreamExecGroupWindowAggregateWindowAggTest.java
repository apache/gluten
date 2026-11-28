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
package org.apache.gluten.planner;

import org.apache.gluten.table.runtime.operators.GlutenWindowOneInputOperator;

import io.github.zhztheplayer.velox4j.plan.GroupWindowAggregationNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Planner-level tests for the gluten override of {@link StreamExecGroupWindowAggregate}: the
 * translated transformation stays non-keyed (the user key belongs to the velox keyed state backend
 * and the upstream exchange, so no Flink state key selector may be declared), the operator is the
 * {@link GlutenWindowOneInputOperator} subclass, each window kind (SESSION / TUMBLE / HOP) plans
 * through to the velox node, and the fail-loud faces: row-count windows, non-default EMIT
 * strategies, processing-time attributes, TIMESTAMP_LTZ attributes, and aggregate calls with a
 * FILTER clause.
 */
public class StreamExecGroupWindowAggregateWindowAggTest {

  @Test
  public void sessionWindowTransformationStaysNonKeyed() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createRowtimeTable(tEnv, Arrays.asList(row(1, 1, 100L), row(2, 2, 200L)));
    Table result =
        tEnv.sqlQuery(
            "SELECT k, COUNT(v) AS c,"
                + " SESSION_START(ts, INTERVAL '10' SECOND) AS ws,"
                + " SESSION_END(ts, INTERVAL '10' SECOND) AS we"
                + " FROM T GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)");
    DataStream<Row> changelog = tEnv.toChangelogStream(result);
    OneInputTransformation<?, ?> windowAgg = findGroupWindowAgg(changelog.getTransformation());
    assertThat(windowAgg)
        .as("the translated plan must contain the group-window-aggregate transformation")
        .isNotNull();
    // A declared Flink state key selector would put the transformation into a
    // keyed context the native keyed state backend does not share (routing
    // through Flink keyed state fails with "Key group not in KeyGroupRange");
    // the user key rides the plan's KeyedStateSpec and the upstream exchange instead.
    assertThat(windowAgg.getStateKeySelector()).isNull();
    assertThat(windowAgg.getStateKeyType()).isNull();
    assertThat(windowAgg.getOperator()).isInstanceOf(GlutenWindowOneInputOperator.class);
    GroupWindowAggregationNode sessionNode =
        windowAggNodeOf(
            tEnv, "SELECT k, COUNT(v) AS c FROM T GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)");
    assertThat(sessionNode.getWindowType()).isEqualTo(2); // SESSION
    assertThat(sessionNode.getGap()).isEqualTo(10_000L);
    assertThat(sessionNode.getSize()).isEqualTo(0L);
    assertThat(sessionNode.getSlide()).isEqualTo(0L);
  }

  @Test
  public void rowCountWindowFailsLoud() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createProctimeTable(tEnv, Arrays.asList(Row.of(1, 1)));
    // A BIGINT window size means a row-count window (CountTumbling), out of
    // scope for the velox group window aggregation - the same check Flink
    // applies to route to the count-window operator family. Row-count
    // windows reach the planner through the Table API (the SQL validator
    // only accepts interval sizes); the check fires before the proctime
    // face.
    Table result =
        tEnv.from("T")
            .window(Tumble.over(lit(5L)).on($("pt")).as("w"))
            .groupBy($("k"), $("w"))
            .select($("k"), $("v").count());
    Throwable thrown = catchThrowable(() -> tEnv.toChangelogStream(result));
    Throwable root = rootCause(thrown);
    assertThat(root)
        .as("a row-count window must fail at translation, got: %s", thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("VELOX_UNSUPPORTED")
        .hasMessageContaining("row-count");
  }

  @Test
  public void earlyFireEmitFailsLoud() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createRowtimeTable(tEnv, Arrays.asList(row(1, 1, 100L)));
    tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.enabled", "true");
    // A positive early-fire delay carries the plan past Flink's own emit
    // validation (session windows and enabled-without-delay are rejected
    // there earlier); a TUMBLE window with a valid strategy reaches the
    // gluten rejection of non-default EMIT.
    tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "1 s");
    Table result =
        tEnv.sqlQuery(
            "SELECT k, COUNT(v) AS c FROM T GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
    Throwable thrown = catchThrowable(() -> tEnv.toChangelogStream(result));
    Throwable root = rootCause(thrown);
    assertThat(root)
        .as("the early-fire EMIT strategy must fail at translation, got: %s", thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("VELOX_UNSUPPORTED")
        .hasMessageContaining("early-fire");
  }

  @Test
  public void allowLatenessFailsLoud() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createRowtimeTable(tEnv, Arrays.asList(row(1, 1, 100L)));
    // Flink's WindowEmitStrategy zeroes allow-lateness for session windows and
    // unless late-fire is enabled, so the guard is exercised through the
    // emitStrategy derivation on a TUMBLE window with late-fire on.
    tEnv.getConfig().getConfiguration().setString("table.exec.emit.allow-lateness", "10 s");
    tEnv.getConfig().getConfiguration().setString("table.exec.emit.late-fire.enabled", "true");
    tEnv.getConfig().getConfiguration().setString("table.exec.emit.late-fire.delay", "1 s");
    Table result =
        tEnv.sqlQuery(
            "SELECT k, COUNT(v) AS c FROM T GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
    Throwable thrown = catchThrowable(() -> tEnv.toChangelogStream(result));
    Throwable root = rootCause(thrown);
    assertThat(root)
        .as("non-zero allowed lateness must fail at translation, got: %s", thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("VELOX_UNSUPPORTED")
        .hasMessageContaining("lateness");
  }

  @Test
  public void proctimeWindowFailsLoud() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createProctimeTable(tEnv, Arrays.asList(Row.of(1, 1)));
    Table result =
        tEnv.sqlQuery(
            "SELECT k, COUNT(v) AS c FROM T GROUP BY k, SESSION(pt, INTERVAL '10' SECOND)");
    Throwable thrown = catchThrowable(() -> tEnv.toChangelogStream(result));
    Throwable root = rootCause(thrown);
    assertThat(root)
        .as("a processing-time window must fail at translation, got: %s", thrown)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("VELOX_UNSUPPORTED")
        .hasMessageContaining("rowtime");
  }

  @Test
  public void tumbleAndHopWindowsPlanToVelox() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createRowtimeTable(tEnv, Arrays.asList(row(1, 1, 100L)));
    // TUMBLE and HOP plan through the gluten override to the velox window
    // aggregation node; assert the window kind and parameters on the plan
    // node, not just that a transformation exists.
    GroupWindowAggregationNode tumble =
        windowAggNodeOf(
            tEnv, "SELECT k, COUNT(v) AS c FROM T GROUP BY k, TUMBLE(ts, INTERVAL '10' SECOND)");
    assertThat(tumble.getWindowType()).isEqualTo(1); // TUMBLE
    assertThat(tumble.getSize()).isEqualTo(10_000L);
    assertThat(tumble.getSlide()).isEqualTo(0L);

    GroupWindowAggregationNode hop =
        windowAggNodeOf(
            tEnv,
            "SELECT k, COUNT(v) AS c FROM T"
                + " GROUP BY k, HOP(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)");
    assertThat(hop.getWindowType()).isEqualTo(0); // HOP
    assertThat(hop.getSize()).isEqualTo(10_000L);
    assertThat(hop.getSlide()).isEqualTo(5_000L);
  }

  @Test
  public void timestampLtzTimeAttributeFailsLoud() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    tEnv.executeSql(
        "CREATE TABLE L (k INT, v INT, ts TIMESTAMP_LTZ(3), WATERMARK FOR ts AS ts)"
            + " WITH ('connector'='datagen','number-of-rows'='1')");
    Table result =
        tEnv.sqlQuery(
            "SELECT k, COUNT(v) AS c FROM L GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)");
    Throwable thrown = catchThrowable(() -> tEnv.toChangelogStream(result));
    assertThat(rootCause(thrown))
        .as("a TIMESTAMP_LTZ time attribute must fail at translation, got: %s", thrown)
        .hasMessageContaining("TIMESTAMP_LTZ");
  }

  @Test
  public void filteredAggregateFailsLoud() {
    assertGlutenOverrideIsUnderTest();
    StreamTableEnvironment tEnv = newTableEnv();
    createRowtimeTable(tEnv, Arrays.asList(row(1, 1, 100L)));
    Table result =
        tEnv.sqlQuery(
            "SELECT k, COUNT(v) FILTER (WHERE v > 0) AS c FROM T"
                + " GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)");
    Throwable thrown = catchThrowable(() -> tEnv.toChangelogStream(result));
    assertThat(rootCause(thrown))
        .as("an aggregate call with a FILTER clause must fail at translation, got: %s", thrown)
        .hasMessageContaining("FILTER");
  }

  /** Unwraps the window aggregation plan node from the translated operator. */
  private static GroupWindowAggregationNode windowAggNodeOf(
      StreamTableEnvironment tEnv, String sql) {
    Table table = tEnv.sqlQuery(sql);
    OneInputTransformation<?, ?> windowAgg =
        findGroupWindowAgg(tEnv.toChangelogStream(table).getTransformation());
    assertThat(windowAgg).isNotNull();
    assertThat(windowAgg.getOperator()).isInstanceOf(GlutenWindowOneInputOperator.class);
    PlanNode node =
        ((GlutenWindowOneInputOperator<?, ?>) windowAgg.getOperator()).getPlanNode().getNode();
    return (GroupWindowAggregationNode) node;
  }

  /**
   * The classpath must resolve the gluten-flink-planner override of the ExecNode, not Flink's own:
   * otherwise these assertions would guard code that never runs. The jar carries the artifact name;
   * an IDE build resolves the module's classes directory.
   */
  private static void assertGlutenOverrideIsUnderTest() {
    String location =
        StreamExecGroupWindowAggregate.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString();
    boolean fromGlutenPlanner =
        location.contains("gluten-flink-planner") || location.contains("/planner/");
    assertThat(fromGlutenPlanner)
        .as(
            "StreamExecGroupWindowAggregate must load from the gluten-flink-planner module, got: %s",
            location)
        .isTrue();
  }

  private static StreamTableEnvironment newTableEnv() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    return StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
  }

  /** A values table over a rowtime attribute (the watermark makes ts a rowtime attribute). */
  private static void createRowtimeTable(StreamTableEnvironment tEnv, List<Row> rows) {
    String dataId = TestValuesTableFactory.registerData(rows);
    tEnv.executeSql(
        "CREATE TABLE T ("
            + " k INT,"
            + " v INT,"
            + " ts TIMESTAMP(3),"
            + " WATERMARK FOR ts AS ts"
            + ") WITH ("
            + " 'connector' = 'values',"
            + " 'bounded' = 'true',"
            + " 'data-id' = '"
            + dataId
            + "')");
  }

  /** A values-table row over (k, v, ts-in-millis): the external type of TIMESTAMP(3). */
  private static Row row(int k, int v, long tsMillis) {
    return Row.of(k, v, LocalDateTime.ofInstant(Instant.ofEpochMilli(tsMillis), ZoneOffset.UTC));
  }

  /** A values table over a proctime attribute. */
  private static void createProctimeTable(StreamTableEnvironment tEnv, List<Row> rows) {
    String dataId = TestValuesTableFactory.registerData(rows);
    tEnv.executeSql(
        "CREATE TABLE T ("
            + " k INT,"
            + " v INT,"
            + " pt AS PROCTIME()"
            + ") WITH ("
            + " 'connector' = 'values',"
            + " 'bounded' = 'true',"
            + " 'data-id' = '"
            + dataId
            + "')");
  }

  private static Table sessionQuery(StreamTableEnvironment tEnv) {
    return tEnv.sqlQuery(
        "SELECT k, COUNT(v) AS c" + " FROM T GROUP BY k, SESSION(ts, INTERVAL '10' SECOND)");
  }

  /** Depth-first walk over the transformation DAG for the group-window-aggregate operator. */
  private static OneInputTransformation<?, ?> findGroupWindowAgg(Transformation<?> transformation) {
    if (transformation instanceof OneInputTransformation) {
      OneInputTransformation<?, ?> one = (OneInputTransformation<?, ?>) transformation;
      String name = one.getName();
      if (name.startsWith("GroupWindowAggregate") || name.startsWith("group-window-aggregate")) {
        return one;
      }
      return findGroupWindowAgg(one.getInputs().get(0));
    }
    for (Transformation<?> input : transformation.getInputs()) {
      OneInputTransformation<?, ?> found = findGroupWindowAgg(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable root = throwable;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    return root;
  }
}
