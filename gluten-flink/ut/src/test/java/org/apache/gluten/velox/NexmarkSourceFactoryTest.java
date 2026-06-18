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
package org.apache.gluten.velox;

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;

import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

public class NexmarkSourceFactoryTest {

  private final NexmarkSourceFactory factory = new NexmarkSourceFactory();

  @Test
  public void testBuildVeloxSourceIgnoresWatermarkPushDownSpec() {
    LegacySourceTransformation<RowData> transformation =
        (LegacySourceTransformation<RowData>)
            factory.buildVeloxSource(
                createSourceTransformation(),
                Map.of(
                    ScanTableSource.class.getName(),
                    new Object(),
                    "checkpoint.enabled",
                    false,
                    "watermarkPushDownSpec",
                    Optional.of(new Object())));

    GlutenStreamSource source = (GlutenStreamSource) transformation.getOperator();
    PlanNode scan = source.getPlanNode().getNode();
    assertThat(scan).isInstanceOf(TableScanNode.class);
    assertThat(scan.getClass().getSimpleName()).isNotEqualTo("TableScanWithWatermarkNode");
  }

  private static SourceTransformation<RowData, TestSplit, Void> createSourceTransformation() {
    RowType rowType =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("event_type", DataTypes.INT()),
                    DataTypes.FIELD("dateTime", DataTypes.TIMESTAMP(3)))
                .getLogicalType();
    return new SourceTransformation<>(
        "NexmarkSource",
        new NexmarkSource(),
        WatermarkStrategy.noWatermarks(),
        InternalTypeInfo.of(rowType),
        1);
  }

  private static class NexmarkSource implements Source<RowData, TestSplit, Void> {
    public List<TestSplit> getSplits(int parallelism) {
      return List.of(new TestSplit());
    }

    @Override
    public Boundedness getBoundedness() {
      return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, TestSplit> createReader(SourceReaderContext readerContext) {
      return new TestSourceReader();
    }

    @Override
    public SplitEnumerator<TestSplit, Void> createEnumerator(
        SplitEnumeratorContext<TestSplit> enumContext) {
      return new TestSplitEnumerator();
    }

    @Override
    public SplitEnumerator<TestSplit, Void> restoreEnumerator(
        SplitEnumeratorContext<TestSplit> enumContext, Void checkpoint) {
      return new TestSplitEnumerator();
    }

    @Override
    public SimpleVersionedSerializer<TestSplit> getSplitSerializer() {
      return new TestSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
      return new TestCheckpointSerializer();
    }
  }

  private static class TestSplit implements SourceSplit {
    private final GeneratorConfig generatorConfig = new GeneratorConfig();

    @Override
    public String splitId() {
      return "test-split";
    }
  }

  private static class GeneratorConfig {
    private final Long maxEvents = 100L;
  }

  private static class TestSourceReader implements SourceReader<RowData, TestSplit> {
    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) {
      return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<TestSplit> snapshotState(long checkpointId) {
      return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<TestSplit> splits) {}

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() {}
  }

  private static class TestSplitEnumerator implements SplitEnumerator<TestSplit, Void> {
    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {}

    @Override
    public void addSplitsBack(List<TestSplit> splits, int subtaskId) {}

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public Void snapshotState(long checkpointId) {
      return null;
    }

    @Override
    public void close() {}
  }

  private static class TestSplitSerializer implements SimpleVersionedSerializer<TestSplit> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(TestSplit split) {
      return new byte[0];
    }

    @Override
    public TestSplit deserialize(int version, byte[] serialized) {
      return new TestSplit();
    }
  }

  private static class TestCheckpointSerializer implements SimpleVersionedSerializer<Void> {
    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(Void checkpoint) {
      return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) {
      return null;
    }
  }
}
