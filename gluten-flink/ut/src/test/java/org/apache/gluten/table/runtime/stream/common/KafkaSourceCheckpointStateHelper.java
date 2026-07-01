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
package org.apache.gluten.table.runtime.stream.common;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class KafkaSourceCheckpointStateHelper {
  public static final String STATE_NAME = "gluten-source-checkpoint-state";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private KafkaSourceCheckpointStateHelper() {}

  public static List<KafkaSourceCheckpointRecord> readLatestKafkaSourceState(
      Path checkpointRoot, String operatorUid) throws Exception {
    List<Path> checkpoints = findCheckpointPointers(checkpointRoot);
    for (int i = checkpoints.size() - 1; i >= 0; i--) {
      List<String> records = readListState(checkpoints.get(i), operatorUid);
      if (!records.isEmpty()) {
        List<KafkaSourceCheckpointRecord> parsed = new ArrayList<>();
        for (String record : records) {
          parsed.add(KafkaSourceCheckpointRecord.parse(record));
        }
        return parsed;
      }
    }
    return List.of();
  }

  public static Path writeKafkaSourceStateSavepoint(
      Path savepointPath, String operatorUid, List<String> records) throws Exception {
    StreamExecutionEnvironment writerEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    writerEnv.setParallelism(1);
    StateBootstrapTransformation<String> transformation =
        OperatorTransformation.bootstrapWith(writerEnv.fromData(records))
            .transform(new KafkaSourceStateBootstrapFunction());

    SavepointWriter.newSavepoint(writerEnv, new HashMapStateBackend(), 128)
        .withOperator(OperatorIdentifier.forUid(operatorUid), transformation)
        .write(savepointPath.toUri().toString());
    writerEnv.execute("write-gluten-kafka-source-state-savepoint");
    return savepointPath;
  }

  public static String kafkaSourceCheckpointRecord(
      String topic, int partition, long offset, long checkpointId, String groupId)
      throws IOException {
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    root.put("connector", "kafka");
    root.put("checkpointId", checkpointId);
    root.put("groupId", groupId);
    ArrayNode topicPartitions = root.putArray("topicPartitions");
    ObjectNode topicPartition = topicPartitions.addObject();
    topicPartition.put("topic", topic);
    topicPartition.put("partition", partition);
    topicPartition.put("offset", offset);
    return OBJECT_MAPPER.writeValueAsString(root);
  }

  private static List<Path> findCheckpointPointers(Path checkpointRoot) throws IOException {
    if (!Files.exists(checkpointRoot)) {
      return List.of();
    }
    try (Stream<Path> paths = Files.walk(checkpointRoot)) {
      return paths
          .filter(path -> path.getFileName().toString().equals("_metadata"))
          .map(Path::getParent)
          .sorted(Comparator.comparing(KafkaSourceCheckpointStateHelper::checkpointId))
          .collect(Collectors.toList());
    }
  }

  private static long checkpointId(Path checkpointPath) {
    String name = checkpointPath.getFileName().toString();
    if (!name.startsWith("chk-")) {
      return Long.MIN_VALUE;
    }
    try {
      return Long.parseLong(name.substring("chk-".length()));
    } catch (NumberFormatException e) {
      return Long.MIN_VALUE;
    }
  }

  private static List<String> readListState(Path checkpointPath, String operatorUid)
      throws Exception {
    StreamExecutionEnvironment readerEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    readerEnv.setParallelism(1);
    SavepointReader reader =
        SavepointReader.read(
            readerEnv, checkpointPath.toUri().toString(), new HashMapStateBackend());
    DataStream<String> state =
        reader.readListState(
            OperatorIdentifier.forUid(operatorUid),
            STATE_NAME,
            Types.STRING,
            StringSerializer.INSTANCE);

    List<String> records = new ArrayList<>();
    try (CloseableIterator<String> iterator = state.executeAndCollect()) {
      while (iterator.hasNext()) {
        records.add(iterator.next());
      }
    }
    return records;
  }

  public static final class KafkaSourceCheckpointRecord {
    private final String connector;
    private final String planNodeId;
    private final long checkpointId;
    private final String groupId;
    private final List<TopicPartitionOffset> topicPartitions;

    private KafkaSourceCheckpointRecord(
        String connector,
        String planNodeId,
        long checkpointId,
        String groupId,
        List<TopicPartitionOffset> topicPartitions) {
      this.connector = connector;
      this.planNodeId = planNodeId;
      this.checkpointId = checkpointId;
      this.groupId = groupId;
      this.topicPartitions = topicPartitions;
    }

    static KafkaSourceCheckpointRecord parse(String record) throws IOException {
      JsonNode root = OBJECT_MAPPER.readTree(record);
      List<TopicPartitionOffset> topicPartitions = new ArrayList<>();
      for (JsonNode topicPartition : root.path("topicPartitions")) {
        topicPartitions.add(
            new TopicPartitionOffset(
                topicPartition.path("topic").asText(),
                topicPartition.path("partition").asInt(),
                topicPartition.path("offset").asLong()));
      }
      return new KafkaSourceCheckpointRecord(
          root.path("connector").asText(),
          root.path("planNodeId").asText(),
          root.path("checkpointId").asLong(),
          root.path("groupId").asText(),
          topicPartitions);
    }

    public String connector() {
      return connector;
    }

    public String planNodeId() {
      return planNodeId;
    }

    public long checkpointId() {
      return checkpointId;
    }

    public String groupId() {
      return groupId;
    }

    public List<TopicPartitionOffset> topicPartitions() {
      return topicPartitions;
    }

    public long offsetSum() {
      return topicPartitions.stream().mapToLong(TopicPartitionOffset::offset).sum();
    }
  }

  public static final class TopicPartitionOffset {
    private final String topic;
    private final int partition;
    private final long offset;

    private TopicPartitionOffset(String topic, int partition, long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    public String topic() {
      return topic;
    }

    public int partition() {
      return partition;
    }

    public long offset() {
      return offset;
    }
  }

  private static final class KafkaSourceStateBootstrapFunction
      extends StateBootstrapFunction<String> implements CheckpointedFunction {
    private final List<String> records = new ArrayList<>();
    private transient ListState<String> state;

    @Override
    public void processElement(String value, Context ctx) {
      records.add(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      state.update(records);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      state =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>(STATE_NAME, StringSerializer.INSTANCE));
    }
  }
}
