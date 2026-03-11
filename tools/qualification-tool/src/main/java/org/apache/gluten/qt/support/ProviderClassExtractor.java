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
 * Extracts and identifies provider/source class from Spark execution plan node descriptions. This
 * is the most reliable method for detecting lakehouse table formats as it directly identifies the
 * data source provider class.
 *
 * <p>Supported provider patterns:
 *
 * <ul>
 *   <li>Iceberg: {@code org.apache.iceberg.spark.source.*}
 *   <li>Delta Lake: {@code io.delta.*} or {@code com.databricks.sql.transaction.tahoe.*}
 *   <li>Hudi: {@code org.apache.hudi.*}
 *   <li>Paimon: {@code org.apache.paimon.*}
 * </ul>
 */
public class ProviderClassExtractor {

  // Provider class patterns - order matters for matching priority
  private static final Pattern ICEBERG_PROVIDER =
      Pattern.compile("org\\.apache\\.iceberg\\.spark\\.source\\.[\\w.]+");

  private static final Pattern DELTA_PROVIDER =
      Pattern.compile("(io\\.delta\\.|com\\.databricks\\.sql\\.transaction\\.tahoe\\.)[\\w.]+");

  private static final Pattern HUDI_PROVIDER = Pattern.compile("org\\.apache\\.hudi\\.[\\w.]+");

  private static final Pattern PAIMON_PROVIDER = Pattern.compile("org\\.apache\\.paimon\\.[\\w.]+");

  /**
   * Extracts the lakehouse format from the node description by matching provider class patterns.
   *
   * @param nodeDesc the full node description
   * @return the detected lakehouse format, or empty if no provider class matches
   */
  public Optional<LakehouseFormat> extract(String nodeDesc) {
    if (nodeDesc == null || nodeDesc.isEmpty()) {
      return Optional.empty();
    }

    // Check patterns in order of specificity
    if (matchesPattern(ICEBERG_PROVIDER, nodeDesc)) {
      return Optional.of(LakehouseFormat.ICEBERG);
    }

    if (matchesPattern(DELTA_PROVIDER, nodeDesc)) {
      return Optional.of(LakehouseFormat.DELTA);
    }

    if (matchesPattern(HUDI_PROVIDER, nodeDesc)) {
      return Optional.of(LakehouseFormat.HUDI);
    }

    if (matchesPattern(PAIMON_PROVIDER, nodeDesc)) {
      return Optional.of(LakehouseFormat.PAIMON);
    }

    return Optional.empty();
  }

  private boolean matchesPattern(Pattern pattern, String text) {
    Matcher matcher = pattern.matcher(text);
    return matcher.find();
  }
}
