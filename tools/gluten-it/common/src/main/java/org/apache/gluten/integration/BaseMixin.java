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
package org.apache.gluten.integration;

import org.apache.gluten.integration.action.Action;
import org.apache.gluten.integration.clickbench.ClickBenchSuite;
import org.apache.gluten.integration.collections.JavaCollectionConverter;
import org.apache.gluten.integration.command.SparkRunModes;
import org.apache.gluten.integration.ds.TpcdsSuite;
import org.apache.gluten.integration.h.TpchSuite;
import org.apache.gluten.integration.metrics.MetricMapper;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import picocli.CommandLine;

import java.io.*;
import java.util.*;

public class BaseMixin {

  @CommandLine.Option(
      required = true,
      names = {"--benchmark-type"},
      description = "Benchmark type: h, ds, clickbench",
      defaultValue = "h")
  private String benchmarkType;

  @CommandLine.Option(
      names = {"-p", "--preset"},
      description = "Preset used: vanilla, velox, velox-with-celeborn, velox-with-uniffle...",
      defaultValue = "velox")
  private String preset;

  @CommandLine.Option(
      names = {"--baseline-preset"},
      description =
          "Baseline preset used: vanilla, velox, velox-with-celeborn, velox-with-uniffle...",
      defaultValue = "vanilla")
  private String baselinePreset;

  @CommandLine.Option(
      names = {"--app-name"},
      description = "The name of Spark application started by the benchmark",
      defaultValue = "Gluten Integration Test")
  private String appName;

  @CommandLine.Option(
      names = {"--log-level"},
      description = "Set log level: DEBUG, INFO, WARN, etc.",
      defaultValue = "WARN")
  private String logLevel;

  @CommandLine.Option(
      names = {"--error-on-memleak"},
      description = "Fail the test when memory leak is detected by Spark's memory manager",
      defaultValue = "false")
  private boolean errorOnMemLeak;

  @CommandLine.Option(
      names = {"--data-source"},
      description = "Datasource used to generate data and to create tables",
      defaultValue = "parquet")
  private String dataSource;

  @CommandLine.Option(
      names = {"--data-dir"},
      description = "Location for storing data used by tests",
      defaultValue = "/tmp")
  private String dataDir;

  @CommandLine.Option(
      names = {"-s", "--scale"},
      description = "The scale factor of sample TPC-H dataset",
      defaultValue = "0.1")
  private double dataScale;

  @CommandLine.Option(
      names = {"--gen-partitioned-data"},
      description = "Generate data with partitions",
      defaultValue = "false")
  private boolean genPartitionedData;

  @CommandLine.Option(
      names = {"--data-gen-features"},
      description =
          "Set a comma-separated list of data generator features to enable. Example: --data-gen-features=enable_dv,delete_10pc",
      split = ",")
  private String[] dataGenFeatures = new String[0];

  @CommandLine.Option(
      names = {"--enable-ui"},
      description = "Enable Spark UI",
      defaultValue = "false")
  private boolean enableUi;

  @CommandLine.Option(
      names = {"--enable-history"},
      description = "Start a Spark history server during running",
      defaultValue = "false")
  private boolean enableHsUi;

  @CommandLine.Option(
      names = {"--history-ui-port"},
      description = "Port that Spark history server UI binds to",
      defaultValue = "18080")
  private int hsUiPort;

  @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
  private SparkRunModes.Mode.Enumeration runModeEnumeration;

  @CommandLine.Option(
      names = {"--disable-aqe"},
      description = "Disable Spark SQL adaptive query execution",
      defaultValue = "false")
  private boolean disableAqe;

  @CommandLine.Option(
      names = {"--disable-bhj"},
      description = "Disable Spark SQL broadcast hash join",
      defaultValue = "false")
  private boolean disableBhj;

  @CommandLine.Option(
      names = {"--disable-wscg"},
      description = "Disable Spark SQL whole stage code generation",
      defaultValue = "false")
  private boolean disableWscg;

  @CommandLine.Option(
      names = {"--enable-cbo"},
      description = "Enable Spark CBO and analyze all tables before running queries",
      defaultValue = "false")
  private boolean enableCbo;

  @CommandLine.Option(
      names = {"--shuffle-partitions"},
      description = "Shuffle partition number",
      defaultValue = "100")
  private int shufflePartitions;

  @CommandLine.Option(
      names = {"--scan-partitions"},
      description =
          "Scan partition number. This is an approximate value, so the actual scan partition number might vary around this value. -1 for letting Spark choose an appropriate number.",
      defaultValue = "-1")
  private int scanPartitions;

  @CommandLine.Option(
      names = {"--decimal-as-double"},
      description = "Generate double values for decimal type column",
      defaultValue = "false")
  private boolean decimalAsDouble;

  @CommandLine.Option(
      names = {"--extra-conf"},
      description =
          "Extra Spark config entries applying to generated Spark session. E.g. --extra-conf=k1=v1 --extra-conf=k2=v2")
  private Map<String, String> extraSparkConf = Collections.emptyMap();

  @CommandLine.Option(
      names = {"--report"},
      description =
          "The file path where the test report will be written. If not specified, the report will be printed to stdout only.",
      defaultValue = "")
  private String reportPath;

  private SparkConf pickSparkConf(String preset) {
    return Preset.get(preset).getConf();
  }

  private MetricMapper pickMetricMapper(String preset) {
    return Preset.get(preset).getMetricMapper();
  }

  public Integer runActions(Action[] actions) {
    final Level level = Level.toLevel(logLevel);
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, level.toString());
    LogManager.getRootLogger().setLevel(level);

    final SparkConf baselineConf = pickSparkConf(baselinePreset);
    final SparkConf testConf = pickSparkConf(preset);

    scala.collection.immutable.Map<String, String> extraSparkConfScala =
        JavaCollectionConverter.asScalaMap(
            mergeMapSafe(extraSparkConf, runModeEnumeration.extraSparkConf()));

    final MetricMapper baselineMetricMapper = pickMetricMapper(baselinePreset);
    final MetricMapper testMetricMapper = pickMetricMapper(preset);

    final Suite suite;
    switch (benchmarkType) {
      case "h":
        suite =
            new TpchSuite(
                appName,
                runModeEnumeration.getSparkMasterUrl(),
                actions,
                testConf,
                baselineConf,
                extraSparkConfScala,
                level,
                errorOnMemLeak,
                dataSource,
                dataDir,
                dataScale,
                genPartitionedData,
                JavaCollectionConverter.asScalaSeq(Arrays.asList(dataGenFeatures)),
                enableUi,
                enableHsUi,
                hsUiPort,
                disableAqe,
                disableBhj,
                disableWscg,
                enableCbo,
                shufflePartitions,
                scanPartitions,
                decimalAsDouble,
                baselineMetricMapper,
                testMetricMapper,
                reportPath);
        break;
      case "ds":
        suite =
            new TpcdsSuite(
                appName,
                runModeEnumeration.getSparkMasterUrl(),
                actions,
                testConf,
                baselineConf,
                extraSparkConfScala,
                level,
                errorOnMemLeak,
                dataSource,
                dataDir,
                dataScale,
                genPartitionedData,
                JavaCollectionConverter.asScalaSeq(Arrays.asList(dataGenFeatures)),
                enableUi,
                enableHsUi,
                hsUiPort,
                disableAqe,
                disableBhj,
                disableWscg,
                enableCbo,
                shufflePartitions,
                scanPartitions,
                decimalAsDouble,
                baselineMetricMapper,
                testMetricMapper,
                reportPath);
        break;
      case "clickbench":
        suite =
            new ClickBenchSuite(
                appName,
                runModeEnumeration.getSparkMasterUrl(),
                actions,
                testConf,
                baselineConf,
                extraSparkConfScala,
                level,
                errorOnMemLeak,
                dataSource,
                dataDir,
                dataScale,
                genPartitionedData,
                enableUi,
                enableHsUi,
                hsUiPort,
                disableAqe,
                disableBhj,
                disableWscg,
                enableCbo,
                shufflePartitions,
                scanPartitions,
                decimalAsDouble,
                baselineMetricMapper,
                testMetricMapper,
                reportPath);
        break;
      default:
        throw new IllegalArgumentException("TPC benchmark type not found: " + benchmarkType);
    }

    // Execute the suite.
    final boolean succeed;
    try {
      succeed = suite.run();
    } finally {
      suite.close();
    }
    if (!succeed) {
      return -1;
    }
    return 0;
  }

  private <K, V> Map<K, V> mergeMapSafe(Map<K, V> conf, Map<? extends K, ? extends V> other) {
    other
        .keySet()
        .forEach(
            k -> {
              if (conf.containsKey(k)) {
                throw new IllegalArgumentException("Key already exists in conf: " + k);
              }
            });

    HashMap<K, V> copy = new HashMap<>(conf);
    copy.putAll(other);
    return Collections.unmodifiableMap(copy);
  }
}
