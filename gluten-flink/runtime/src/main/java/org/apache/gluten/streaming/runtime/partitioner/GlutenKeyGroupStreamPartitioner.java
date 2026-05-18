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
package org.apache.gluten.streaming.runtime.partitioner;

import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Partitioner selects the target channel based on the key group index.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class GlutenKeyGroupStreamPartitioner extends StreamPartitioner<StatefulRecord>
    implements ConfigurableStreamPartitioner {
  private static final long serialVersionUID = 1L;
  private final KeySelector<StatefulRecord, Integer> keySelector;

  private int maxParallelism;
  private int parallelism;

  public GlutenKeyGroupStreamPartitioner(
      KeySelector<StatefulRecord, Integer> keySelector, int maxParallelism, int parallelism) {
    Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
    this.keySelector = Preconditions.checkNotNull(keySelector);
    this.maxParallelism = maxParallelism;
    this.parallelism = parallelism;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  @Override
  public int selectChannel(SerializationDelegate<StreamRecord<StatefulRecord>> record) {
    try {
      int key = keySelector.getKey(record.getInstance().getValue());
      int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
      return KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
          maxParallelism, parallelism, keyGroup);
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not extract key from " + record.getInstance().getValue(), e);
    }
  }

  @Override
  public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
    return SubtaskStateMapper.RANGE;
  }

  @Override
  public StreamPartitioner<StatefulRecord> copy() {
    return this;
  }

  @Override
  public boolean isPointwise() {
    return false;
  }

  @Override
  public String toString() {
    return "GlutenHASH";
  }

  @Override
  public void configure(int maxParallelism) {
    KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
    this.maxParallelism = maxParallelism;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final GlutenKeyGroupStreamPartitioner that = (GlutenKeyGroupStreamPartitioner) o;
    return maxParallelism == that.maxParallelism && keySelector.equals(that.keySelector);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), keySelector, maxParallelism);
  }
}
