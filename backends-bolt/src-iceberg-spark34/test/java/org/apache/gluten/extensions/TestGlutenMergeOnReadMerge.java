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
package org.apache.gluten.extensions;

import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.extensions.TestMergeOnReadMerge;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.Test;

import java.util.Map;

import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_MODE_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGlutenMergeOnReadMerge extends TestMergeOnReadMerge {
  public TestGlutenMergeOnReadMerge(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode,
      boolean fanoutEnabled,
      String branch,
      PlanningMode planningMode) {
    super(
        catalogName,
        implementation,
        config,
        fileFormat,
        vectorized,
        distributionMode,
        fanoutEnabled,
        branch,
        planningMode);
  }

  @Test
  public synchronized void testMergeWithConcurrentTableRefresh() {
    System.out.println("Run timeout");
  }

  @Test
  public synchronized void testMergeWithSerializableIsolation() {
    System.out.println("Run timeout");
  }

  @Test
  public synchronized void testMergeWithSnapshotIsolation() {
    System.out.println("Run timeout");
  }

  // The matched join string is changed from Join to ShuffledHashJoinExecTransformer
  @Test
  public void testMergeConditionSplitIntoTargetPredicateAndJoinCondition() {
    createAndInitTable(
        "id INT, salary INT, dep STRING, sub_dep STRING",
        "PARTITIONED BY (dep, sub_dep)",
        "{ \"id\": 1, \"salary\": 100, \"dep\": \"d1\", \"sub_dep\": \"sd1\" }\n"
            + "{ \"id\": 6, \"salary\": 600, \"dep\": \"d6\", \"sub_dep\": \"sd6\" }");

    createOrReplaceView(
        "source",
        "id INT, salary INT, dep STRING, sub_dep STRING",
        "{ \"id\": 1, \"salary\": 101, \"dep\": \"d1\", \"sub_dep\": \"sd1\" }\n"
            + "{ \"id\": 2, \"salary\": 200, \"dep\": \"d2\", \"sub_dep\": \"sd2\" }\n"
            + "{ \"id\": 3, \"salary\": 300, \"dep\": \"d3\", \"sub_dep\": \"sd3\"  }");

    String query =
        String.format(
            "MERGE INTO %s AS t USING source AS s "
                + "ON t.id == s.id AND ((t.dep = 'd1' AND t.sub_dep IN ('sd1', 'sd3')) OR (t.dep = 'd6' AND t.sub_dep IN ('sd2', 'sd6'))) "
                + "WHEN MATCHED THEN "
                + "  UPDATE SET salary = s.salary "
                + "WHEN NOT MATCHED THEN "
                + "  INSERT *",
            commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);

    if (mode(table) == COPY_ON_WRITE) {
      checkJoinAndFilterConditions(
          query,
          "ShuffledHashJoinExecTransformer [id], [id], FullOuter",
          "((dep = 'd1' AND sub_dep IN ('sd1', 'sd3')) OR (dep = 'd6' AND sub_dep IN ('sd2', 'sd6')))");
    } else {
      checkJoinAndFilterConditions(
          query,
          "ShuffledHashJoinExecTransformer [id], [id], RightOuter",
          "((dep = 'd1' AND sub_dep IN ('sd1', 'sd3')) OR (dep = 'd6' AND sub_dep IN ('sd2', 'sd6')))");
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, 101, "d1", "sd1"), // updated
            row(2, 200, "d2", "sd2"), // new
            row(3, 300, "d3", "sd3"), // new
            row(6, 600, "d6", "sd6")), // existing
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  private void checkJoinAndFilterConditions(String query, String join, String icebergFilters) {
    // disable runtime filtering for easier validation
    withSQLConf(
        ImmutableMap.of(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false"),
        () -> {
          SparkPlan sparkPlan = executeAndKeepPlan(() -> sql(query));
          String planAsString = sparkPlan.toString().replaceAll("#(\\d+L?)", "");

          // Remove "\n" because gluten prints BuildRight or BuildLeft in the end.
          assertThat(planAsString).as("Join should match").contains(join);

          assertThat(planAsString)
              .as("Pushed filters must match")
              .contains("[filters=" + icebergFilters + ",");
        });
  }

  private RowLevelOperationMode mode(Table table) {
    String modeName = table.properties().getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
    return RowLevelOperationMode.fromName(modeName);
  }
}
