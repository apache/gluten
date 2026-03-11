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

import org.apache.gluten.qt.support.LakehouseFormatDetector.LakehouseFormat;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

/** Unit tests for {@link LakehouseFormatDetector}. */
public class LakehouseFormatDetectorTest {

  private LakehouseFormatDetector detector;

  @Before
  public void setUp() {
    detector = new LakehouseFormatDetector();
  }

  // ==================== Iceberg Detection Tests ====================

  @Test
  public void testDetectIcebergFromProviderClass() {
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] IcebergScan\n"
            + "org.apache.iceberg.spark.source.SparkBatchQueryScan@12345\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("BatchScanExec", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.ICEBERG, result.get());
  }

  @Test
  public void testDetectIcebergFromNodeName() {
    String nodeDesc = "Format: Parquet, ReadSchema: struct<col1:string>";

    Optional<LakehouseFormat> result = detector.detect("IcebergScan", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.ICEBERG, result.get());
  }

  // ==================== Delta Lake Detection Tests ====================

  @Test
  public void testDetectDeltaFromProviderClass() {
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "io.delta.tables.DeltaTable\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.DELTA, result.get());
  }

  @Test
  public void testDetectDeltaFromTahoeProviderClass() {
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "com.databricks.sql.transaction.tahoe.DeltaLog\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.DELTA, result.get());
  }

  @Test
  public void testDetectDeltaFromLocationPath() {
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: Parquet,\n"
            + "Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/sales/_delta_log],\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.DELTA, result.get());
  }

  @Test
  public void testDetectDeltaFromNodeName() {
    String nodeDesc = "Format: Parquet, ReadSchema: struct<col1:string>";

    Optional<LakehouseFormat> result = detector.detect("DeltaScan", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.DELTA, result.get());
  }

  // ==================== Hudi Detection Tests ====================

  @Test
  public void testDetectHudiFromProviderClass() {
    String nodeDesc =
        "HoodieFileScan [col1#0, col2#1]\n"
            + "org.apache.hudi.HoodieSparkUtils\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("HoodieFileScan", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.HUDI, result.get());
  }

  @Test
  public void testDetectHudiFromLocationPath() {
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "Location: s3://bucket/table/.hoodie/metadata,\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.HUDI, result.get());
  }

  @Test
  public void testDetectHudiFromNodeName() {
    String nodeDesc = "Format: Parquet, ReadSchema: struct<col1:string>";

    Optional<LakehouseFormat> result = detector.detect("HudiFileScan", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.HUDI, result.get());
  }

  // ==================== Paimon Detection Tests ====================

  @Test
  public void testDetectPaimonFromProviderClass() {
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1] PaimonScan\n"
            + "org.apache.paimon.spark.SparkSource\n"
            + "Format: Parquet, ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("BatchScanExec", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.PAIMON, result.get());
  }

  @Test
  public void testDetectPaimonFromNodeName() {
    String nodeDesc = "Format: Parquet, ReadSchema: struct<col1:string>";

    Optional<LakehouseFormat> result = detector.detect("PaimonScan", nodeDesc);

    assertTrue(result.isPresent());
    assertEquals(LakehouseFormat.PAIMON, result.get());
  }

  // ==================== Raw Format Tests (No Lakehouse) ====================

  @Test
  public void testRawParquetScan() {
    String nodeDesc =
        "FileScan parquet [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: Parquet,\n"
            + "Location: s3://bucket/raw_parquet_table,\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", nodeDesc);

    assertFalse(result.isPresent());
  }

  @Test
  public void testRawOrcScan() {
    String nodeDesc =
        "FileScan orc [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: ORC,\n"
            + "Location: hdfs://cluster/raw_orc_table,\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan orc", nodeDesc);

    assertFalse(result.isPresent());
  }

  // ==================== False Positive Prevention Tests ====================

  @Test
  public void testTableNamedDeltaSalesNotDetectedAsDelta() {
    // A raw Parquet table named "delta_sales" should NOT be detected as Delta
    String nodeDesc =
        "FileScan parquet delta_sales [col1#0, col2#1]\n"
            + "Batched: true, DataFilters: [], Format: Parquet,\n"
            + "Location: s3://bucket/delta_sales,\n"
            + "ReadSchema: struct<col1:string,col2:int>";

    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", nodeDesc);

    // Should NOT detect as Delta since there's no provider class or _delta_log
    assertFalse(result.isPresent());
  }

  @Test
  public void testGenericBatchScanExecNotDetected() {
    // Generic V2 scan (e.g., Kafka) should NOT be detected as lakehouse
    String nodeDesc =
        "BatchScanExec [col1#0, col2#1]\n"
            + "org.apache.spark.sql.kafka010.KafkaSourceProvider\n"
            + "Format: kafka, ReadSchema: struct<key:binary,value:binary>";

    Optional<LakehouseFormat> result = detector.detect("BatchScanExec", nodeDesc);

    assertFalse(result.isPresent());
  }

  // ==================== File Format Extraction Tests ====================

  @Test
  public void testExtractParquetFormat() {
    String nodeDesc = "Format: Parquet, ReadSchema: struct<col1:string>";

    String format = detector.extractFileFormat(nodeDesc);

    assertEquals("Parquet", format);
  }

  @Test
  public void testExtractOrcFormat() {
    String nodeDesc = "Format: ORC, ReadSchema: struct<col1:string>";

    String format = detector.extractFileFormat(nodeDesc);

    assertEquals("ORC", format);
  }

  @Test
  public void testExtractAvroFormat() {
    String nodeDesc = "Format: Avro, ReadSchema: struct<col1:string>";

    String format = detector.extractFileFormat(nodeDesc);

    assertEquals("Avro", format);
  }

  @Test
  public void testExtractFormatMissing() {
    String nodeDesc = "ReadSchema: struct<col1:string>";

    String format = detector.extractFileFormat(nodeDesc);

    assertEquals("", format);
  }

  @Test
  public void testExtractFormatNullDesc() {
    String format = detector.extractFileFormat(null);

    assertEquals("", format);
  }

  // ==================== Edge Cases ====================

  @Test
  public void testNullNodeName() {
    String nodeDesc = "Format: Parquet";

    Optional<LakehouseFormat> result = detector.detect(null, nodeDesc);

    assertFalse(result.isPresent());
  }

  @Test
  public void testNullNodeDesc() {
    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", null);

    assertFalse(result.isPresent());
  }

  @Test
  public void testEmptyNodeDesc() {
    Optional<LakehouseFormat> result = detector.detect("FileScan parquet", "");

    assertFalse(result.isPresent());
  }
}
