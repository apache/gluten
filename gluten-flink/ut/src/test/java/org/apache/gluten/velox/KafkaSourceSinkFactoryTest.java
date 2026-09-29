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

import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanWithWatermarkNode;
import io.github.zhztheplayer.velox4j.plan.WatermarkPushDownSpec;
import io.github.zhztheplayer.velox4j.type.IntegerType;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
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
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSourceSinkFactoryTest {

  private final KafkaSourceSinkFactory factory = new KafkaSourceSinkFactory();

  @Test
  public void testBuildVeloxSourceWithoutWatermarkPushDownUsesTableScan() {
    PlanNode scan = buildScanNode(Optional.empty());

    assertThat(scan).isInstanceOf(TableScanNode.class);
    assertThat(scan).isNotInstanceOf(TableScanWithWatermarkNode.class);
  }

  @Test
  public void testBuildVeloxSourceWithWatermarkPushDownUsesTableScanWithWatermark() {
    WatermarkPushDownSpec watermarkPushDownSpec = createWatermarkPushDownSpec();

    PlanNode scan = buildScanNode(Optional.of(watermarkPushDownSpec));

    assertThat(scan).isInstanceOf(TableScanWithWatermarkNode.class);
    assertThat(((TableScanWithWatermarkNode) scan).getWatermarkPushDownSpec())
        .isSameAs(watermarkPushDownSpec);
  }

  private PlanNode buildScanNode(Optional<WatermarkPushDownSpec> watermarkPushDownSpec) {
    LegacySourceTransformation<RowData> transformation =
        (LegacySourceTransformation<RowData>)
            factory.buildVeloxSource(
                createSourceTransformation(),
                Map.of(
                    ScanTableSource.class.getName(),
                    createKafkaDynamicSource(),
                    "checkpoint.enabled",
                    false,
                    "watermarkPushDownSpec",
                    watermarkPushDownSpec));

    GlutenStreamSource source = (GlutenStreamSource) transformation.getOperator();
    return source.getPlanNode().getNode();
  }

  private static SourceTransformation<RowData, TestSplit, Void> createSourceTransformation() {
    RowType rowType =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("name", DataTypes.STRING()))
                .getLogicalType();
    return new SourceTransformation<>(
        "KafkaSource",
        new KafkaSource(),
        WatermarkStrategy.noWatermarks(),
        InternalTypeInfo.of(rowType),
        1);
  }

  private static KafkaDynamicSource createKafkaDynamicSource() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test-group");
    properties.setProperty("client.id.prefix", "test-client");

    return new KafkaDynamicSource(
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("name", DataTypes.STRING())),
        null,
        new TestDecodingFormat(),
        new int[0],
        new int[] {0, 1},
        null,
        List.of("test-topic"),
        null,
        properties,
        StartupMode.EARLIEST,
        Map.of(),
        0L,
        BoundedMode.UNBOUNDED,
        Map.of(),
        0L,
        false,
        "test-table");
  }

  private static WatermarkPushDownSpec createWatermarkPushDownSpec() {
    IntegerType integerType = new IntegerType();
    io.github.zhztheplayer.velox4j.type.RowType rowType =
        new io.github.zhztheplayer.velox4j.type.RowType(List.of("id"), List.of(integerType));
    ProjectNode project =
        new ProjectNode(
            "watermark_project",
            List.of(new EmptyNode(rowType)),
            List.of("watermark"),
            List.of(FieldAccessTypedExpr.create(integerType, "id")));
    return new WatermarkPushDownSpec(project, 1000L, 200L, -1);
  }

  private static class TestDecodingFormat
      implements DecodingFormat<DeserializationSchema<RowData>> {
    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
        DynamicTableSource.Context context, DataType producedDataType) {
      throw new UnsupportedOperationException("Runtime decoder is not needed by this test.");
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }
  }

  private static class KafkaSource implements Source<RowData, TestSplit, Void> {
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
    @Override
    public String splitId() {
      return "test-split";
    }
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
    public byte[] serialize(Void checkpoint) throws IOException {
      return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
      return null;
    }
  }
}
