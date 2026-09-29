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

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenStreamingFileWriterOperator;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaToHiveE2ETest {

  private static final int KAFKA_PORT = 19092;
  private static final String VELOX_SOURCE_SINK_FACTORY_SERVICE =
      "META-INF/services/org.apache.gluten.velox.VeloxSourceSinkFactory";

  @RegisterExtension
  static final SharedKafkaTestResource KAFKA =
      new SharedKafkaTestResource()
          .withBrokerProperty("host.name", "127.0.0.1")
          .withBrokers(1)
          .registerListener(new PlainListener().onPorts(KAFKA_PORT));

  @Test
  void testInsertFromKafkaToHive(@TempDir Path tempDir) throws Exception {
    String topic = "kafka_to_hive_" + UUID.randomUUID().toString().replace("-", "");
    KAFKA.getKafkaTestUtils().createTopic(topic, 1, (short) 1);
    KAFKA
        .getKafkaTestUtils()
        .produceRecords(
            List.of(
                jsonRecord(topic, "{\"id\":1,\"name\":\"alice\"}"),
                jsonRecord(topic, "{\"id\":2,\"name\":\"bob\"}"),
                jsonRecord(topic, "{\"id\":3,\"name\":\"carol\"}")));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());

    createKafkaSource(tEnv, topic);
    Path sinkPath = tempDir.resolve("hive_sink");
    createHiveCatalogAndSink(tEnv, sinkPath);

    String insertSql =
        "INSERT INTO hive_catalog.`default`.hive_sink "
            + "SELECT id, name FROM default_catalog.default_database.kafka_source";
    assertHiveSinkOffloadedToNative(tEnv, insertSql);
    TableResult insertResult = executeSqlWithoutVeloxSourceSinkFactories(tEnv, insertSql);
    JobClient jobClient = insertResult.getJobClient().orElseThrow();
    jobClient.getJobExecutionResult().get(60000, TimeUnit.MILLISECONDS);

    assertThat(readHiveTextRows(sinkPath)).containsExactlyInAnyOrder("1,alice", "2,bob", "3,carol");
  }

  @Test
  void testCommitHivePartitionWithProcessTime(@TempDir Path tempDir) throws Exception {
    testCommitHivePartition(tempDir, "process-time");
  }

  @Test
  void testCommitHivePartitionWithPartitionTime(@TempDir Path tempDir) throws Exception {
    testCommitHivePartition(tempDir, "partition-time");
  }

  private void testCommitHivePartition(Path tempDir, String partitionCommitTrigger)
      throws Exception {
    String topic =
        "kafka_to_partitioned_hive_"
            + partitionCommitTrigger.replace("-", "_")
            + "_"
            + UUID.randomUUID().toString().replace("-", "");
    KAFKA.getKafkaTestUtils().createTopic(topic, 1, (short) 1);
    KAFKA
        .getKafkaTestUtils()
        .produceRecords(
            List.of(
                jsonRecord(
                    topic, "{\"id\":1,\"name\":\"alice\",\"dt\":\"2026-06-29\",\"hm\":\"10:00\"}"),
                jsonRecord(
                    topic, "{\"id\":2,\"name\":\"bob\",\"dt\":\"2026-06-29\",\"hm\":\"10:00\"}"),
                jsonRecord(
                    topic,
                    "{\"id\":3,\"name\":\"carol\",\"dt\":\"2026-06-29\",\"hm\":\"10:01\"}")));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv =
        StreamTableEnvironment.create(
            env, EnvironmentSettings.newInstance().inStreamingMode().build());

    createPartitionedKafkaSource(tEnv, topic);
    Path sinkPath = tempDir.resolve("partitioned_hive_sink");
    createHiveCatalogAndPartitionedSink(tEnv, sinkPath, partitionCommitTrigger);

    TableResult insertResult =
        executeSqlWithoutVeloxSourceSinkFactories(
            tEnv,
            "INSERT INTO hive_catalog.`default`.partitioned_hive_sink "
                + "SELECT id, name, dt, hm FROM default_catalog.default_database.kafka_source");
    JobClient jobClient = insertResult.getJobClient().orElseThrow();
    jobClient.getJobExecutionResult().get(60000, TimeUnit.MILLISECONDS);

    assertThat(readHiveTextRows(sinkPath)).containsExactlyInAnyOrder("1,alice", "2,bob", "3,carol");
    assertCommittedPartitions(tEnv);
    assertCommittedPartition(sinkPath, "2026-06-29", "10:00");
    assertCommittedPartition(sinkPath, "2026-06-29", "10:01");
  }

  private static ProducerRecord<byte[], byte[]> jsonRecord(String topic, String value) {
    return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
  }

  private void createKafkaSource(StreamTableEnvironment tEnv, String topic) {
    tEnv.executeSql(
        "CREATE TABLE kafka_source ("
            + " id INT,"
            + " name STRING"
            + ") WITH ("
            + " 'connector' = 'kafka',"
            + " 'topic' = '"
            + topic
            + "',"
            + " 'properties.bootstrap.servers' = '"
            + "127.0.0.1:"
            + KAFKA_PORT
            + "',"
            + " 'properties.group.id' = 'kafka-to-hive-e2e',"
            + " 'properties.broker.address.family' = 'v4',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset',"
            + " 'format' = 'json'"
            + ")");
  }

  private void createPartitionedKafkaSource(StreamTableEnvironment tEnv, String topic) {
    tEnv.executeSql(
        "CREATE TABLE kafka_source ("
            + " id INT,"
            + " name STRING,"
            + " dt STRING,"
            + " hm STRING"
            + ") WITH ("
            + " 'connector' = 'kafka',"
            + " 'topic' = '"
            + topic
            + "',"
            + " 'properties.bootstrap.servers' = '"
            + "127.0.0.1:"
            + KAFKA_PORT
            + "',"
            + " 'properties.group.id' = 'kafka-to-partitioned-hive-e2e',"
            + " 'properties.broker.address.family' = 'v4',"
            + " 'scan.startup.mode' = 'earliest-offset',"
            + " 'scan.bounded.mode' = 'latest-offset',"
            + " 'format' = 'json'"
            + ")");
  }

  private void createHiveCatalogAndSink(StreamTableEnvironment tEnv, Path sinkPath) {
    HiveCatalog hiveCatalog = HiveTestUtils.createHiveCatalog("hive_catalog", "2.3.9");
    tEnv.registerCatalog("hive_catalog", hiveCatalog);

    tEnv.useCatalog("hive_catalog");
    tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    tEnv.executeSql(
        "CREATE TABLE hive_sink (id INT, name STRING) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
            + "STORED AS TEXTFILE LOCATION '"
            + sinkPath.toUri()
            + "'");
    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    tEnv.useCatalog("default_catalog");
  }

  private void createHiveCatalogAndPartitionedSink(
      StreamTableEnvironment tEnv, Path sinkPath, String partitionCommitTrigger) {
    HiveCatalog hiveCatalog = HiveTestUtils.createHiveCatalog("hive_catalog", "2.3.9");
    tEnv.registerCatalog("hive_catalog", hiveCatalog);

    tEnv.useCatalog("hive_catalog");
    tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    tEnv.executeSql(
        "CREATE TABLE partitioned_hive_sink (id INT, name STRING) "
            + "PARTITIONED BY (dt STRING, hm STRING) "
            + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
            + "STORED AS TEXTFILE LOCATION '"
            + sinkPath.toUri()
            + "' "
            + "TBLPROPERTIES ("
            + " 'sink.partition-commit.trigger' = '"
            + partitionCommitTrigger
            + "',"
            + " 'sink.partition-commit.delay' = '0 s',"
            + " 'sink.partition-commit.policy.kind' = 'metastore,success-file',"
            + " 'partition.time-extractor.timestamp-pattern' = '$dt $hm:00'"
            + ")");
    tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    tEnv.useCatalog("default_catalog");
  }

  private static TableResult executeSqlWithoutVeloxSourceSinkFactories(
      StreamTableEnvironment tEnv, String sql) {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread()
        .setContextClassLoader(new VeloxSourceSinkFactoryFilteringClassLoader(originalClassLoader));
    try {
      return tEnv.executeSql(sql);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private static class VeloxSourceSinkFactoryFilteringClassLoader extends ClassLoader {
    private VeloxSourceSinkFactoryFilteringClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    public URL getResource(String name) {
      if (VELOX_SOURCE_SINK_FACTORY_SERVICE.equals(name)) {
        return null;
      }
      return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      if (VELOX_SOURCE_SINK_FACTORY_SERVICE.equals(name)) {
        return Collections.emptyEnumeration();
      }
      return super.getResources(name);
    }
  }

  private static List<String> readHiveTextRows(Path sinkPath) throws Exception {
    try (java.util.stream.Stream<Path> paths = Files.walk(sinkPath)) {
      return paths
          .filter(Files::isRegularFile)
          .filter(path -> !path.getFileName().toString().startsWith("."))
          .filter(path -> !path.getFileName().toString().startsWith("_"))
          .flatMap(
              path -> {
                try {
                  return Files.lines(path, StandardCharsets.UTF_8);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(java.util.stream.Collectors.toList());
    }
  }

  private static void assertCommittedPartitions(StreamTableEnvironment tEnv) throws Exception {
    tEnv.useCatalog("hive_catalog");
    List<String> partitions = new ArrayList<>();
    try (CloseableIterator<Row> rows =
        tEnv.executeSql("SHOW PARTITIONS partitioned_hive_sink").collect()) {
      while (rows.hasNext()) {
        partitions.add(String.valueOf(rows.next().getField(0)));
      }
    } finally {
      tEnv.useCatalog("default_catalog");
    }
    assertThat(partitions)
        .containsExactlyInAnyOrder("dt=2026-06-29/hm=10:00", "dt=2026-06-29/hm=10:01");
  }

  private static void assertCommittedPartition(Path sinkPath, String dt, String hm) {
    Path partitionPath = sinkPath.resolve("dt=" + dt).resolve("hm=" + hm.replace(":", "%3A"));
    assertThat(partitionPath).isDirectory();
    assertThat(partitionPath.resolve("_SUCCESS")).exists();
  }

  private static void assertHiveSinkOffloadedToNative(StreamTableEnvironment tEnv, String insertSql)
      throws Exception {
    assertThat(
            translateInsert(tEnv, insertSql).stream()
                .anyMatch(KafkaToHiveE2ETest::containsNativeHiveSink))
        .as("Hive HDFS sink should use GlutenStreamingFileWriterOperator")
        .isTrue();
  }

  @SuppressWarnings("unchecked")
  private static List<Transformation<?>> translateInsert(
      StreamTableEnvironment tEnv, String insertSql) throws Exception {
    TableEnvironmentImpl tableEnvironment = (TableEnvironmentImpl) tEnv;
    List<Operation> operations = tableEnvironment.getParser().parse(insertSql);
    assertThat(operations).hasSize(1);
    assertThat(operations.get(0)).isInstanceOf(ModifyOperation.class);

    Method translate = TableEnvironmentImpl.class.getDeclaredMethod("translate", List.class);
    translate.setAccessible(true);
    return (List<Transformation<?>>)
        translate.invoke(tableEnvironment, List.of((ModifyOperation) operations.get(0)));
  }

  private static boolean containsNativeHiveSink(Transformation<?> transformation) {
    if (transformation instanceof OneInputTransformation) {
      OneInputTransformation<?, ?> oneInputTransformation =
          (OneInputTransformation<?, ?>) transformation;
      if (oneInputTransformation.getOperatorFactory() instanceof GlutenOneInputOperatorFactory) {
        GlutenOneInputOperatorFactory<?, ?> operatorFactory =
            (GlutenOneInputOperatorFactory<?, ?>) oneInputTransformation.getOperatorFactory();
        if (operatorFactory.getOperator() instanceof GlutenStreamingFileWriterOperator) {
          return true;
        }
      }
    }
    return transformation.getInputs().stream().anyMatch(KafkaToHiveE2ETest::containsNativeHiveSink);
  }
}
