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
package org.apache.gluten.integration.command;

import org.apache.gluten.integration.QuerySet;
import org.apache.gluten.integration.Suite;
import org.apache.gluten.integration.action.Actions;
import org.apache.gluten.integration.collections.JavaCollectionConverter;

import com.google.common.base.Preconditions;
import picocli.CommandLine;

import java.util.*;

public class QueriesMixin {
  @CommandLine.Option(
      names = {"--queries"},
      description =
          "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6",
      split = ",")
  private String[] queries = new String[0];

  @CommandLine.Option(
      names = {"--excluded-queries"},
      description =
          "Set a comma-separated list of query IDs to exclude. Example: --exclude-queries=q1,q6",
      split = ",")
  private String[] excludedQueries = new String[0];

  @CommandLine.Option(
      names = {"--shard"},
      description =
          "Divide the queries to execute into N shards, then pick one single shard and run it. Example: --shard=1/3",
      defaultValue = "1/1")
  private String shard;

  @CommandLine.Option(
      names = {"--explain"},
      description = "Output explain result for queries",
      defaultValue = "false")
  private boolean explain;

  @CommandLine.Option(
      names = {"--iterations"},
      description = "How many iterations to run",
      defaultValue = "1")
  private int iterations;

  @CommandLine.Option(
      names = {"--no-session-reuse"},
      description = "Recreate new Spark session each time a query is about to run",
      defaultValue = "false")
  private boolean noSessionReuse;

  @CommandLine.Option(
      names = {"--suppress-failure-messages"},
      description = "Do not printing failures on error",
      defaultValue = "false")
  private boolean suppressFailureMessages;

  public boolean explain() {
    return explain;
  }

  public int iterations() {
    return iterations;
  }

  public boolean noSessionReuse() {
    return noSessionReuse;
  }

  public boolean suppressFailureMessages() {
    return suppressFailureMessages;
  }

  public Actions.QuerySelector queries() {
    return new Actions.QuerySelector() {
      @Override
      public QuerySet select(Suite suite) {
        final String[] queryIds = queries;
        final String[] excludedQueryIds = excludedQueries;
        if (queryIds.length > 0 && excludedQueryIds.length > 0) {
          throw new IllegalArgumentException(
              "Should not specify queries and excluded queries at the same time");
        }
        QuerySet querySet = suite.allQueries();
        if (queryIds.length > 0) {
          querySet = querySet.filter(JavaCollectionConverter.asScalaSeq(Arrays.asList(queryIds)));
        }
        if (excludedQueryIds.length > 0) {
          querySet =
              querySet.exclude(JavaCollectionConverter.asScalaSeq(Arrays.asList(excludedQueryIds)));
        }
        final Division div = Division.parse(shard);
        querySet = querySet.getShard(div.shard - 1, div.shardCount);
        System.out.println("About to run queries: " + querySet.queryIds().mkString(",") + "... ");
        return querySet;
      }
    };
  }

  private static class Division {
    private final int shard;
    private final int shardCount;

    private Division(int shard, int shardCount) {
      this.shard = shard;
      this.shardCount = shardCount;
    }

    private static Division parse(String shard) {
      String[] parts = shard.split("/");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid shard: " + shard);
      }
      int s = Integer.parseInt(parts[0]);
      int c = Integer.parseInt(parts[1]);
      Preconditions.checkArgument(s >= 1 && c >= 1 && s <= c, "Invalid picked shard: " + shard);
      return new Division(s, c);
    }
  }
}
