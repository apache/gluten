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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects lakehouse table formats by matching location path patterns in node descriptions. This is
 * a fallback detection method when provider class information is not available.
 *
 * <p>This is particularly useful for:
 *
 * <ul>
 *   <li>Delta Lake: Often appears as {@code FileScan parquet} with {@code _delta_log} in the
 *       location path
 *   <li>Hudi: May have {@code .hoodie} marker directory in the location path
 * </ul>
 *
 * <p>Note: Iceberg and Paimon typically include provider class information in the node description,
 * so location-based detection is less reliable for these formats.
 */
public class LocationPatternMatcher {

  // Extract location path from node description
  private static final Pattern LOCATION_PATTERN = Pattern.compile("Location: ([^,\\]]+)");

  // Delta Lake marker - _delta_log directory indicates a Delta table
  private static final Pattern DELTA_LOCATION = Pattern.compile("_delta_log");

  // Hudi marker - .hoodie directory indicates a Hudi table
  private static final Pattern HUDI_LOCATION = Pattern.compile("\\.hoodie");

  /**
   * Matches the node description against known location patterns for lakehouse formats.
   *
   * @param nodeDesc the full node description
   * @return the detected lakehouse format, or empty if no location pattern matches
   */
  public Optional<LakehouseFormat> match(String nodeDesc) {
    if (nodeDesc == null || nodeDesc.isEmpty()) {
      return Optional.empty();
    }

    // Extract location path first
    String location = extractLocation(nodeDesc);
    if (location.isEmpty()) {
      // If no Location field, check the entire description for patterns
      // This handles cases where the path might be in a different format
      location = nodeDesc;
    }

    // Check for Delta Lake pattern
    if (matchesPattern(DELTA_LOCATION, location)) {
      return Optional.of(LakehouseFormat.DELTA);
    }

    // Check for Hudi pattern
    if (matchesPattern(HUDI_LOCATION, location)) {
      return Optional.of(LakehouseFormat.HUDI);
    }

    return Optional.empty();
  }

  /**
   * Extracts the location path from the node description.
   *
   * @param nodeDesc the node description
   * @return the extracted location path, or empty string if not found
   */
  private String extractLocation(String nodeDesc) {
    Matcher matcher = LOCATION_PATTERN.matcher(nodeDesc);
    return matcher.find() ? matcher.group(1) : "";
  }

  private boolean matchesPattern(Pattern pattern, String text) {
    Matcher matcher = pattern.matcher(text);
    return matcher.find();
  }
}
