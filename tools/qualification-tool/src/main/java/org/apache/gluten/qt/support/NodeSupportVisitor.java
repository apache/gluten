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

import org.apache.gluten.qt.graph.SparkPlanGraphNodeInternal;
import org.apache.gluten.qt.support.LakehouseFormatDetector.LakehouseFormat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.gluten.qt.support.NotSupportedCategory.NODE_NOT_SUPPORTED;

/**
 * {@link GraphVisitor} implementation that inspects each node in the {@code ExecutionDescription}
 * to determine whether it is fully supported by Gluten. Specifically, this visitor classifies each
 * node as either {@link Supported} or {@link NotSupported} based on:
 *
 * <ul>
 *   <li>The operator name (e.g., {@code BroadcastHashJoin}, {@code Scan})
 *   <li>Data formats (e.g., Parquet, ORC) and their unsupported types
 *   <li>Resource constraints (e.g., broadcast data size)
 * </ul>
 *
 * The aggregated results are then stored in {@code resultSupportMap} and incorporated into a new
 * {@link ExecutionDescription} instance.
 */
public class NodeSupportVisitor extends GraphVisitor {
  private static final Pattern MEM_PATTERN =
      Pattern.compile("^\\d+\\.?\\d*\\s*(EiB|PiB|TiB|GiB|MiB|KiB|B)\\b");
  private static final Pattern SIZE_PATTERN = Pattern.compile("([\\d.]+)\\s*([a-zA-Z]+)");
  private static final Pattern FORMAT_PATTERN = Pattern.compile("Format: ([^,]+)");
  private static final Pattern SCHEMA_PATTERN = Pattern.compile("ReadSchema: struct<(.*)");

  private final LakehouseFormatDetector lakehouseDetector = new LakehouseFormatDetector();

  private static final ImmutableSet<String> FULLY_SUPPORTED_OPERATORS =
      ImmutableSet.of(
          "AdaptiveSparkPlan",
          "TakeOrderedAndProject",
          "Project",
          "Filter",
          "Window",
          "Coalesce",
          "CartesianProduct",
          "HashAggregate",
          "SortAggregate",
          "Sort",
          "ObjectHashAggregate",
          "Expand",
          "Union",
          "ShuffleExchange",
          "ShuffledHashJoin",
          "SortMergeJoin",
          "BroadcastExchange",
          "GlobalLimit",
          "LocalLimit",
          "Generate",
          "EvalPython",
          "AQEShuffleRead",
          "ReusedExchange",
          "Subquery",
          "SubqueryBroadcast",
          "ShuffleQueryStage",
          "Exchange",
          "BroadcastQueryStage",
          "ColumnarToRow",
          "RowToColumnar",
          "ReusedSubquery",
          "ColumnarExchange",
          "ColumnarBroadcastExchange",
          "VeloxColumnarToRowExec",
          "RowToVeloxColumnar",
          "ColumnarSubqueryBroadcast",
          "ColumnarUnion",
          "InMemoryTableScan",
          "BroadcastNestedLoopJoin",
          "CollectLimit");

  public NodeSupportVisitor(ExecutionDescription executionDescription) {
    super(executionDescription);
  }

  @Override
  protected void visitor(long nodeId) {
    resultSupportMap.put(nodeId, isSupported(nodeId));
  }

  private String schemaConditions(String schema, List<String> conditions) {
    return conditions.stream()
        .map(cond -> ":" + cond)
        .filter(schema::contains)
        .collect(Collectors.joining(" and "))
        .replace(":", "");
  }

  private Optional<String> extractSize(String input) {
    Matcher matcher = MEM_PATTERN.matcher(input);

    if (matcher.find()) {
      return Optional.of(matcher.group());
    } else {
      return Optional.empty();
    }
  }

  private Optional<BigInteger> stringToBytes(String sizeStr) {
    Matcher matcher = SIZE_PATTERN.matcher(sizeStr);
    if (matcher.matches()) {
      BigDecimal size = new BigDecimal(matcher.group(1));
      String unit = matcher.group(2);
      switch (unit) {
        case "EiB":
          return Optional.of(size.multiply(BigDecimal.valueOf(1L << 60)).toBigInteger());
        case "PiB":
          return Optional.of(size.multiply(BigDecimal.valueOf(1L << 50)).toBigInteger());
        case "TiB":
          return Optional.of(size.multiply(BigDecimal.valueOf(1L << 40)).toBigInteger());
        case "GiB":
          return Optional.of(size.multiply(BigDecimal.valueOf(1L << 30)).toBigInteger());
        case "MiB":
          return Optional.of(size.multiply(BigDecimal.valueOf(1L << 20)).toBigInteger());
        case "KiB":
          return Optional.of(size.multiply(BigDecimal.valueOf(1L << 10)).toBigInteger());
        case "B":
          return Optional.of(size.toBigInteger());
        default:
          return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private static final ImmutableList<String> PARQUET_UNSUPPORTED_TYPES =
      ImmutableList.of("struct", "map", "list", "tinyint");

  private static final ImmutableList<String> ORC_UNSUPPORTED_TYPES =
      ImmutableList.of("struct", "map", "list", "tinyint", "timestamp", "varchar", "char", "union");

  private boolean checkTypeSupport(String dataType, List<String> unsupportedList) {
    return unsupportedList.stream().map(s -> ":" + s).noneMatch(dataType::contains);
  }

  private GlutenSupport isSupported(long nodeId) {
    SparkPlanGraphNodeInternal node = executionDescription.getNode(nodeId);
    String name = node.getName();
    String anonymizedName = node.getAnonymizedName();
    String desc = node.getDesc();

    if (FULLY_SUPPORTED_OPERATORS.contains(name)) {
      return new Supported();
    } else if (name.equals("BroadcastHashJoin")) {
      boolean bhjSupported =
          executionDescription.getChildren(nodeId).stream()
              .map(executionDescription::getNode)
              .filter(childNode -> childNode.getName().equals("BroadcastExchange"))
              .flatMap(
                  childNode ->
                      childNode.getMetrics().stream()
                          .filter(metric -> metric.getName().contains("data size")))
              .flatMap(
                  metric ->
                      Optional.ofNullable(
                          executionDescription.getMetrics(metric.getAccumulatorId()))
                          .stream())
              .flatMap(
                  metricStr ->
                      Arrays.stream(metricStr.split("\n", -1))
                          .reduce((first, second) -> second)
                          .stream())
              .flatMap(metricStr -> extractSize(metricStr).stream())
              .flatMap(metricStr -> stringToBytes(metricStr).stream())
              .anyMatch(size -> size.compareTo(BigInteger.valueOf(100L << 20)) <= 0);
      return bhjSupported
          ? new Supported()
          : new NotSupported(NODE_NOT_SUPPORTED, "BHJ Not Supported");
    } else if (name.contains("Scan")) {
      // Try to detect lakehouse format first
      Optional<LakehouseFormat> lakehouseFormat = lakehouseDetector.detect(name, desc);

      Matcher schemaMatcher = SCHEMA_PATTERN.matcher(desc);
      String schema = schemaMatcher.find() ? schemaMatcher.group(1) : "";

      if (lakehouseFormat.isPresent()) {
        // Lakehouse format detected - check underlying file format
        String fileFormat = lakehouseDetector.extractFileFormat(desc);
        String formatPrefix = lakehouseFormat.get().getDisplayName() + " Scan";

        if (fileFormat.isEmpty()) {
          return new NotSupported(
              NODE_NOT_SUPPORTED, formatPrefix + " with format UNKNOWN not supported");
        }

        // Lakehouse formats use Parquet/ORC underneath - apply same type checks
        if (fileFormat.equals("Parquet") && checkTypeSupport(schema, PARQUET_UNSUPPORTED_TYPES)) {
          return new Supported();
        } else if (fileFormat.equals("ORC") && checkTypeSupport(schema, ORC_UNSUPPORTED_TYPES)) {
          return new Supported();
        } else if (fileFormat.equals("Parquet")) {
          return new NotSupported(
              NODE_NOT_SUPPORTED,
              formatPrefix + " with " + schemaConditions(schema, PARQUET_UNSUPPORTED_TYPES));
        } else if (fileFormat.equals("ORC")) {
          return new NotSupported(
              NODE_NOT_SUPPORTED,
              formatPrefix + " with " + schemaConditions(schema, ORC_UNSUPPORTED_TYPES));
        } else {
          // Unsupported underlying format (e.g., Avro for Iceberg)
          return new NotSupported(
              NODE_NOT_SUPPORTED, formatPrefix + " (" + fileFormat + ") not supported");
        }
      }

      // Not a lakehouse format - fall back to raw format detection
      Matcher formatMatcher = FORMAT_PATTERN.matcher(desc);
      String format = formatMatcher.find() ? formatMatcher.group(1) : "";

      if (format.equals("Parquet") && checkTypeSupport(schema, PARQUET_UNSUPPORTED_TYPES)) {
        return new Supported();
      } else if (format.equals("ORC") && checkTypeSupport(schema, ORC_UNSUPPORTED_TYPES)) {
        return new Supported();
      } else if (format.equals("Parquet")) {
        return new NotSupported(
            NODE_NOT_SUPPORTED,
            "Parquet with " + schemaConditions(schema, PARQUET_UNSUPPORTED_TYPES));
      } else if (format.equals("ORC")) {
        return new NotSupported(
            NODE_NOT_SUPPORTED, "Orc with " + schemaConditions(schema, ORC_UNSUPPORTED_TYPES));
      } else {
        return new NotSupported(
            NODE_NOT_SUPPORTED,
            anonymizedName
                + " with format "
                + (format.isBlank() ? "UNKNOWN" : format)
                + " not supported");
      }
    } else if (name.matches(".*Transformer$")) {
      return new Supported();
    } else {
      return new NotSupported(NODE_NOT_SUPPORTED, anonymizedName + " not supported");
    }
  }
}
