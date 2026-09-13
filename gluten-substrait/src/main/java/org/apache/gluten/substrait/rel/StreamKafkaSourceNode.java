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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.proto.StreamKafka;
import org.apache.gluten.utils.SubstraitUtil;

import io.substrait.proto.ReadRel;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamKafkaSourceNode implements SplitInfo {

  private final String topic;
  private final Integer partition;
  private final Long startOffset;
  private final Long endOffset;
  private final Long pollTimeoutMs;
  private final Boolean failOnDataLoss;
  private final Boolean includeHeaders;

  private final Map<String, Object> kafkaParams;

  public StreamKafkaSourceNode(
      String topic,
      Integer partition,
      Long startOffset,
      Long endOffset,
      Long pollTimeoutMs,
      Boolean failOnDataLoss,
      Boolean includeHeaders,
      Map<String, Object> kafkaParams) {
    this.topic = topic;
    this.partition = partition;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.pollTimeoutMs = pollTimeoutMs;
    this.failOnDataLoss = failOnDataLoss;
    this.includeHeaders = includeHeaders;
    this.kafkaParams = kafkaParams;
  }

  @Override
  public List<String> preferredLocations() {
    return Collections.emptyList();
  }

  @Override
  public ReadRel.ExtensionTable toProtobuf() {
    StreamKafka.Builder builder = StreamKafka.newBuilder();

    StreamKafka.TopicPartition.Builder topicPartition = StreamKafka.TopicPartition.newBuilder();
    topicPartition.setTopic(topic);
    topicPartition.setPartition(partition);

    builder.setTopicPartition(topicPartition.build());
    builder.setStartOffset(startOffset);
    builder.setEndOffset(endOffset);
    builder.setPollTimeoutMs(pollTimeoutMs);
    builder.setFailOnDataLoss(failOnDataLoss);
    builder.setIncludeHeaders(includeHeaders);
    kafkaParams.forEach((k, v) -> builder.putParams(k, v.toString()));

    return SubstraitUtil.packExtensionTable(builder.build());
  }
}
