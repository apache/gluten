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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.table.runtime.stream.common.KafkaSourceCheckpointStateHelper;
import org.apache.gluten.table.runtime.stream.common.KafkaSourceCheckpointStateHelper.KafkaSourceCheckpointRecord;
import org.apache.gluten.table.runtime.stream.common.KafkaSourceCheckpointStateHelper.TopicPartitionOffset;
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;
import org.apache.gluten.velox.KafkaSourceSinkFactory;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end checkpoint restore coverage for Gluten's native Kafka source.
 *
 * <p>The test runs against the docker-compose Kafka broker at {@code kafka:9092}. It creates a
 * random topic, starts a Flink MiniCluster streaming job through the Table/SQL Kafka connector
 * path, and lets {@link org.apache.gluten.velox.KafkaSourceSinkFactory} replace the Flink source
 * with the Gluten native source.
 *
 * <p>The job consumes a first batch, waits until a checkpoint is completed, and then fails once
 * from a downstream operator. After Flink restarts the job, the test writes a second batch while
 * the Kafka source must restore from the source offset records stored in Flink operator state.
 * Finally, it verifies that all records from both batches are delivered, proving the snapshot and
 * restore path for native Kafka source offsets is exercised.
 */
public class KafkaSourceCheckpointIT {
  private static final String BOOTSTRAP_SERVERS =
      System.getProperty("gluten.flink.kafka.bootstrap.servers", "kafka:9092");
  private static final int FIRST_BATCH_SIZE = 20;
  private static final int SECOND_BATCH_SIZE = 20;
  private static final int RECORD_COUNT = FIRST_BATCH_SIZE + SECOND_BATCH_SIZE;
  private static final int RESTORED_OFFSET = 10;

  private static final Set<Integer> RESULTS = ConcurrentHashMap.newKeySet();
  private static final AtomicInteger SEEN_BY_FAILING_MAP = new AtomicInteger();
  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
  private static final AtomicBoolean FIRST_BATCH_CHECKPOINTED = new AtomicBoolean();

  private static MiniClusterWithClientResource miniCluster;

  @BeforeAll
  static void beforeAll() throws Exception {
    Velox4jEnvironment.initializeOnce();
    miniCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    miniCluster.before();
  }

  @AfterAll
  static void afterAll() {
    if (miniCluster != null) {
      miniCluster.after();
    }
  }

  @Test
  void testKafkaSourceRestoresOffsetStateAfterFailure() throws Exception {
    RESULTS.clear();
    SEEN_BY_FAILING_MAP.set(0);
    FAILED_ONCE.set(false);
    FIRST_BATCH_CHECKPOINTED.set(false);

    String topic = "gluten-flink-source-checkpoint-" + UUID.randomUUID();
    String groupId = "gluten-flink-source-checkpoint-group-" + UUID.randomUUID();
    Path checkpointDir = Files.createTempDirectory("gluten-kafka-source-checkpoint-it");

    try (AdminClient admin = AdminClient.create(kafkaProperties())) {
      admin
          .createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }

    Configuration configuration = new Configuration();
    configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    env.setParallelism(1);
    env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig()
        .setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Duration.ofSeconds(5)));

    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    tableEnv.executeSql(createKafkaSourceDdl(topic, groupId));

    // Pipeline shape:
    // Kafka topic -> Gluten native Kafka source -> gluten-calc -> TableToDataStream
    //     -> JVM fail-once map -> JVM collecting sink.
    // The source and calc exercise Gluten's native path, while the downstream map/sink stay in
    // Flink JVM operators so the test can trigger a deterministic failure after checkpoint
    // completion and collect the restored output.
    Table table = tableEnv.sqlQuery("SELECT id FROM kafka_source");
    DataStream<Row> rows = tableEnv.toDataStream(table);
    // This map operator deliberately fails the task once in notifyCheckpointComplete().
    // The failure happens after the source has produced records and Flink has completed a
    // checkpoint, forcing the restarted job to restore the Gluten Kafka source offset state.
    rows.map(new FailOnceAfterCheckpoint())
        .name("fail-once-after-checkpoint")
        .addSink(new CollectingSink())
        .name("collect-results");

    JobClient jobClient = env.executeAsync("gluten-kafka-source-checkpoint-it");
    try {
      Thread.sleep(2000);
      produce(topic, 0, FIRST_BATCH_SIZE);
      waitUntil(
          () -> RESULTS.size() >= FIRST_BATCH_SIZE,
          Duration.ofSeconds(30),
          "first Kafka batch records");
      waitUntil(
          () -> checkpointStateContainsFirstBatchOffsets(checkpointDir, topic, groupId),
          Duration.ofSeconds(30),
          "checkpoint containing the first Kafka batch offsets");
      FIRST_BATCH_CHECKPOINTED.set(true);
      waitUntil(
          () -> FAILED_ONCE.get(), Duration.ofSeconds(30), "first checkpoint-triggered failure");

      produce(topic, FIRST_BATCH_SIZE, RECORD_COUNT);
      waitUntil(() -> RESULTS.size() >= RECORD_COUNT, Duration.ofSeconds(60), "all Kafka records");

      assertThat(RESULTS).containsExactlyInAnyOrderElementsOf(expectedIds());
    } finally {
      jobClient.cancel().get(30, TimeUnit.SECONDS);
      try (AdminClient admin = AdminClient.create(kafkaProperties())) {
        admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void testKafkaSourceStartsFromManuallyModifiedCheckpointState() throws Exception {
    RESULTS.clear();

    String topic = "gluten-flink-source-manual-checkpoint-" + UUID.randomUUID();
    String groupId = "gluten-flink-source-manual-checkpoint-group-" + UUID.randomUUID();
    Path savepointPath = Files.createTempDirectory("gluten-kafka-source-manual-savepoint");

    try (AdminClient admin = AdminClient.create(kafkaProperties())) {
      admin
          .createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }

    produce(topic, 0, RECORD_COUNT);
    KafkaSourceCheckpointStateHelper.writeKafkaSourceStateSavepoint(
        savepointPath,
        KafkaSourceSinkFactory.GLUTEN_KAFKA_SOURCE_UID,
        List.of(
            KafkaSourceCheckpointStateHelper.kafkaSourceCheckpointRecord(
                topic, 0, RESTORED_OFFSET, 1, groupId)));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());
    tableEnv.executeSql(createKafkaSourceDdl(topic, groupId));

    Table table = tableEnv.sqlQuery("SELECT id FROM kafka_source");
    DataStream<Row> rows = tableEnv.toDataStream(table);
    rows.map(value -> ((Number) value.getField(0)).intValue())
        .name("extract-id")
        .addSink(new CollectingSink())
        .name("collect-results");

    StreamGraph streamGraph = env.getStreamGraph();
    streamGraph.setSavepointRestoreSettings(
        SavepointRestoreSettings.forPath(savepointPath.toUri().toString(), false));
    streamGraph.setJobName("gluten-kafka-source-manual-checkpoint-state-it");

    JobClient jobClient = env.executeAsync(streamGraph);
    try {
      waitUntil(
          () -> RESULTS.size() >= RECORD_COUNT - RESTORED_OFFSET,
          Duration.ofSeconds(60),
          "Kafka records after the restored offset");

      assertThat(RESULTS).containsExactlyInAnyOrderElementsOf(expectedIds(RESTORED_OFFSET));
    } finally {
      jobClient.cancel().get(30, TimeUnit.SECONDS);
      try (AdminClient admin = AdminClient.create(kafkaProperties())) {
        admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
      }
    }
  }

  private static boolean checkpointStateContainsFirstBatchOffsets(
      Path checkpointDir, String topic, String groupId) throws Exception {
    List<KafkaSourceCheckpointRecord> records =
        KafkaSourceCheckpointStateHelper.readLatestKafkaSourceState(
            checkpointDir, KafkaSourceSinkFactory.GLUTEN_KAFKA_SOURCE_UID);
    if (records.isEmpty()) {
      return false;
    }
    for (KafkaSourceCheckpointRecord record : records) {
      assertThat(record.connector()).isEqualTo("kafka");
      assertThat(record.planNodeId()).isNotEmpty();
      assertThat(record.checkpointId()).isGreaterThanOrEqualTo(0L);
      assertThat(record.groupId()).isEqualTo(groupId);
      assertThat(record.topicPartitions()).isNotEmpty();
      for (TopicPartitionOffset topicPartition : record.topicPartitions()) {
        assertThat(topicPartition.topic()).isEqualTo(topic);
        assertThat(topicPartition.partition()).isGreaterThanOrEqualTo(0);
        assertThat(topicPartition.offset()).isGreaterThanOrEqualTo(0L);
      }
    }
    return records.stream().mapToLong(KafkaSourceCheckpointRecord::offsetSum).sum()
        == FIRST_BATCH_SIZE;
  }

  private static String createKafkaSourceDdl(String topic, String groupId) {
    return "CREATE TABLE kafka_source ("
        + " id INT,"
        + " payload STRING"
        + ") WITH ("
        + " 'connector' = 'kafka',"
        + " 'topic' = '"
        + topic
        + "',"
        + " 'properties.bootstrap.servers' = '"
        + BOOTSTRAP_SERVERS
        + "',"
        + " 'properties.group.id' = '"
        + groupId
        + "',"
        + " 'scan.startup.mode' = 'latest-offset',"
        + " 'format' = 'json'"
        + ")";
  }

  private static void produce(String topic, int startInclusive, int endExclusive) throws Exception {
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(kafkaProperties(), new StringSerializer(), new StringSerializer())) {
      for (int id = startInclusive; id < endExclusive; id++) {
        producer.send(
            new ProducerRecord<>(
                topic, Integer.toString(id), "{\"id\":" + id + ",\"payload\":\"v-" + id + "\"}"));
      }
      producer.flush();
    }
  }

  private static Properties kafkaProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
    return properties;
  }

  private static List<Integer> expectedIds() {
    return expectedIds(0);
  }

  private static List<Integer> expectedIds(int startInclusive) {
    List<Integer> expected = new ArrayList<>();
    for (int id = startInclusive; id < RECORD_COUNT; id++) {
      expected.add(id);
    }
    return expected;
  }

  private static void waitUntil(CheckedBooleanSupplier condition, Duration timeout, String event)
      throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    throw new AssertionError("Timed out waiting for " + event + ". Current results: " + RESULTS);
  }

  private interface CheckedBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  private static class FailOnceAfterCheckpoint extends RichMapFunction<Row, Integer>
      implements CheckpointListener {
    @Override
    public Integer map(Row value) {
      SEEN_BY_FAILING_MAP.incrementAndGet();
      return ((Number) value.getField(0)).intValue();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      // Throwing here is the intentional task failure in this IT. Since this callback only runs
      // after a checkpoint completes, the following restart must restore the native Kafka source
      // offsets from Flink operator state instead of starting from the table's latest-offset mode.
      if (FIRST_BATCH_CHECKPOINTED.get()
          && SEEN_BY_FAILING_MAP.get() > 0
          && FAILED_ONCE.compareAndSet(false, true)) {
        throw new RuntimeException("Fail once after checkpoint " + checkpointId);
      }
    }
  }

  private static class CollectingSink implements SinkFunction<Integer> {
    @Override
    public void invoke(Integer value, Context context) {
      RESULTS.add(value);
    }
  }
}
