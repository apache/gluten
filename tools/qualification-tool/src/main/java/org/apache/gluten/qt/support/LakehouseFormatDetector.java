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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects lakehouse table formats (Iceberg, Delta Lake, Hudi, Paimon) from Spark execution plan
 * scan nodes. This class uses a multi-signal detection approach combining:
 *
 * <ul>
 *   <li>Provider class extraction from node description
 *   <li>Location path pattern matching (fallback for Delta/Hudi)
 *   <li>Node name pattern matching (secondary signal)
 * </ul>
 *
 * <p>Detection is performed in order of reliability: provider class > location pattern > node name.
 */
public class LakehouseFormatDetector {

  private static final Pattern FORMAT_PATTERN = Pattern.compile("Format: ([^,\\]]+)");

  private final ProviderClassExtractor providerExtractor;
  private final LocationPatternMatcher locationMatcher;

  public LakehouseFormatDetector() {
    this.providerExtractor = new ProviderClassExtractor();
    this.locationMatcher = new LocationPatternMatcher();
  }

  /**
   * Detects the lakehouse format from a scan node.
   *
   * @param nodeName the operator name (e.g., "BatchScanExec", "FileScan parquet")
   * @param nodeDesc the full node description containing provider class, location, format, etc.
   * @return the detected format result, or empty if not a lakehouse scan
   */
  public Optional<LakehouseFormat> detect(String nodeName, String nodeDesc) {
    // First try provider class extraction (most reliable)
    Optional<LakehouseFormat> fromProvider = providerExtractor.extract(nodeDesc);
    if (fromProvider.isPresent()) {
      return fromProvider;
    }

    // Then try location pattern matching (for Delta/Hudi without provider class)
    Optional<LakehouseFormat> fromLocation = locationMatcher.match(nodeDesc);
    if (fromLocation.isPresent()) {
      return fromLocation;
    }

    // Finally try node name patterns (least reliable, but catches some cases)
    return detectFromNodeName(nodeName);
  }

  /**
   * Detects lakehouse format from node name patterns. This is the least reliable method and should
   * only be used as a fallback.
   */
  private Optional<LakehouseFormat> detectFromNodeName(String nodeName) {
    if (nodeName == null) {
      return Optional.empty();
    }

    String lowerName = nodeName.toLowerCase();

    // Check for explicit lakehouse scan names
    if (lowerName.contains("icebergscan") || lowerName.contains("iceberg")) {
      return Optional.of(LakehouseFormat.ICEBERG);
    }
    if (lowerName.contains("deltascan") || lowerName.contains("tahoefile")) {
      return Optional.of(LakehouseFormat.DELTA);
    }
    if (lowerName.contains("hudifilescan")
        || lowerName.contains("hoodiefilescan")
        || lowerName.contains("hoodie")) {
      return Optional.of(LakehouseFormat.HUDI);
    }
    if (lowerName.contains("paimonscan") || lowerName.contains("paimon")) {
      return Optional.of(LakehouseFormat.PAIMON);
    }

    return Optional.empty();
  }

  /**
   * Extracts the underlying file format from node description.
   *
   * @param nodeDesc the node description
   * @return the file format (e.g., "Parquet", "ORC"), or empty string if not found
   */
  public String extractFileFormat(String nodeDesc) {
    if (nodeDesc == null) {
      return "";
    }
    Matcher matcher = FORMAT_PATTERN.matcher(nodeDesc);
    return matcher.find() ? matcher.group(1).trim() : "";
  }

  /** Represents the detected lakehouse table format. */
  public enum LakehouseFormat {
    ICEBERG("Iceberg"),
    DELTA("Delta"),
    HUDI("Hudi"),
    PAIMON("Paimon");

    private final String displayName;

    LakehouseFormat(String displayName) {
      this.displayName = displayName;
    }

    public String getDisplayName() {
      return displayName;
    }
  }
}
