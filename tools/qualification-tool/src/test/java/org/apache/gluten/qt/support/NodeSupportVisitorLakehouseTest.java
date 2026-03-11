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
package org.apache.gluten.qt.support;

import org.apache.gluten.qt.graph.MetricInternal;
import org.apache.gluten.qt.graph.SparkPlanGraphInternal;
import org.apache.gluten.qt.graph.SparkPlanGraphNodeInternal;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for {@link NodeSupportVisitor} with lakehouse format scan nodes. These tests
 * verify that the visitor correctly identifies and evaluates support for Iceberg, Delta Lake, Hudi,
 * and Paimon scan nodes.
 */
public class NodeSupportVisitorLakehouseTest {

  // ==================== Iceberg Integration Tests ====================

  @Test
  public void testIcebergParquetScanSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] IcebergScan\n"
            + "org.apache.iceberg.spark.source.SparkBatchQueryScan@12345\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Iceberg Parquet scan should be supported", support instanceof Supported);
  }

  @Test
  public void testIcebergOrcScanSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] IcebergScan\n"
            + "org.apache.iceberg.spark.source.SparkBatchQueryScan@12345\n"
            + "Format: ORC, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Iceberg ORC scan should be supported", support instanceof Supported);
  }

  @Test
  public void testIcebergAvroScanNotSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] IcebergScan\n"
            + "org.apache.iceberg.spark.source.SparkBatchQueryScan@12345\n"
            + "Format: Avro, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Iceberg Avro scan should not be supported", support instanceof NotSupported);
    assertTrue(
        "Should mention Iceberg and Avro",
        ((NotSupported) support)
                .getCategoryReason(NotSupportedCategory.NODE_NOT_SUPPORTED)
                .contains("Iceberg")
            && ((NotSupported) support)
                .getCategoryReason(NotSupportedCategory.NODE_NOT_SUPPORTED)
                .contains("Avro"));
  }

  @Test
  public void testIcebergParquetWithUnsupportedTypeNotSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] IcebergScan\n"
            + "org.apache.iceberg.spark.source.SparkBatchQueryScan@12345\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:map<string,int>>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue(
        "Iceberg Parquet with map type should not be supported", support instanceof NotSupported);
    assertTrue(
        "Should mention map type",
        ((NotSupported) support)
            .getCategoryReason(NotSupportedCategory.NODE_NOT_SUPPORTED)
            .contains("map"));
  }

  // ==================== Delta Lake Integration Tests ====================

  @Test
  public void testDeltaLakeScanWithProviderSupported() {
    String nodeName = "FileScan parquet";
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "io.delta.tables.DeltaTable\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Delta Lake scan with provider should be supported", support instanceof Supported);
  }

  @Test
  public void testDeltaLakeScanWithLocationSupported() {
    String nodeName = "FileScan parquet";
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: Parquet,\n"
            + "Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/sales/_delta_log],\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue(
        "Delta Lake scan with _delta_log location should be supported",
        support instanceof Supported);
  }

  @Test
  public void testDeltaLakeScanWithUnsupportedTypeNotSupported() {
    String nodeName = "FileScan parquet";
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "io.delta.tables.DeltaTable\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,nested:struct<a:int,b:int>>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue(
        "Delta Lake scan with struct type should not be supported",
        support instanceof NotSupported);
    assertTrue(
        "Should mention struct type",
        ((NotSupported) support)
            .getCategoryReason(NotSupportedCategory.NODE_NOT_SUPPORTED)
            .contains("struct"));
  }

  // ==================== Hudi Integration Tests ====================

  @Test
  public void testHudiScanWithProviderSupported() {
    String nodeName = "HoodieFileScan";
    String nodeDesc =
        "HoodieFileScan [col1#0, col2#1]\n"
            + "org.apache.hudi.HoodieSparkUtils\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Hudi scan with provider should be supported", support instanceof Supported);
  }

  @Test
  public void testHudiScanWithLocationSupported() {
    String nodeName = "FileScan parquet";
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "Location: s3://bucket/table/.hoodie/metadata,\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Hudi scan with .hoodie location should be supported", support instanceof Supported);
  }

  // ==================== Paimon Integration Tests ====================

  @Test
  public void testPaimonParquetScanSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] PaimonScan\n"
            + "org.apache.paimon.spark.SparkSource\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Paimon Parquet scan should be supported", support instanceof Supported);
  }

  @Test
  public void testPaimonOrcScanSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] PaimonScan\n"
            + "org.apache.paimon.spark.SparkSource\n"
            + "Format: ORC, ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Paimon ORC scan should be supported", support instanceof Supported);
  }

  // ==================== Raw Format Tests (No Lakehouse) ====================

  @Test
  public void testRawParquetScanSupported() {
    String nodeName = "FileScan parquet";
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: Parquet,\n"
            + "Location: s3://bucket/raw_parquet_table,\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Raw Parquet scan should be supported", support instanceof Supported);
  }

  @Test
  public void testRawOrcScanSupported() {
    String nodeName = "Scan orc";
    String nodeDesc =
        "FileScan orc [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: ORC,\n"
            + "Location: hdfs://cluster/raw_orc_table,\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue("Raw ORC scan should be supported", support instanceof Supported);
  }

  // ==================== False Positive Prevention Tests ====================

  @Test
  public void testTableNamedDeltaSalesNotMisclassified() {
    // A raw Parquet table with "delta" in the name should NOT be treated as Delta Lake
    String nodeName = "FileScan parquet delta_sales";
    String nodeDesc =
        "FileScan parquet delta_sales [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: Parquet,\n"
            + "Location: s3://bucket/delta_sales,\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    // Should be supported as raw Parquet (not misclassified as Delta)
    assertTrue(
        "Table named delta_sales should be supported as raw Parquet", support instanceof Supported);
  }

  @Test
  public void testGenericBatchScanExecNotMisclassified() {
    // Generic V2 scan (e.g., Kafka) should NOT be treated as lakehouse
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1]\n"
            + "org.apache.spark.sql.kafka010.KafkaSourceProvider\n"
            + "Format: kafka, ReadSchema: struct<key:binary,value:binary>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    // Should NOT be supported (not a lakehouse format, unknown format)
    assertTrue("Kafka BatchScanExec should not be supported", support instanceof NotSupported);
  }

  // ==================== Missing Format Field Tests ====================

  @Test
  public void testIcebergScanWithMissingFormatNotSupported() {
    String nodeName = "BatchScanExec";
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] IcebergScan\n"
            + "org.apache.iceberg.spark.source.SparkBatchQueryScan@12345\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    ExecutionDescription execDesc = createSingleNodeExecution(1L, nodeName, nodeDesc);
    NodeSupportVisitor visitor = new NodeSupportVisitor(execDesc);
    ExecutionDescription result = visitor.visitAndTag();

    GlutenSupport support = result.getNodeIdToGluttenSupportMap().get(1L);
    assertTrue(
        "Iceberg scan with missing format should not be supported",
        support instanceof NotSupported);
    assertTrue(
        "Should mention UNKNOWN format",
        ((NotSupported) support)
            .getCategoryReason(NotSupportedCategory.NODE_NOT_SUPPORTED)
            .contains("UNKNOWN"));
  }

  // ==================== Helper Methods ====================

  /** Creates an ExecutionDescription with a single scan node for testing. */
  private ExecutionDescription createSingleNodeExecution(
      long nodeId, String nodeName, String nodeDesc) {
    SparkPlanGraphNodeInternal node =
        new SparkPlanGraphNodeInternal(nodeId, nodeName, nodeDesc, Collections.emptyList());

    List<SparkPlanGraphNodeInternal> nodeList = List.of(node);
    SparkPlanGraphInternal graph =
        new SparkPlanGraphInternal(
            nodeList, // nodes
            Collections.emptyList(), // edges
            nodeList // allNodes
            );

    List<MetricInternal> metrics = Collections.emptyList();

    return new ExecutionDescription(graph, metrics);
  }
}
