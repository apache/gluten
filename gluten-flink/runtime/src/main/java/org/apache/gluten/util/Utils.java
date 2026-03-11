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
package org.apache.gluten.util;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;

import org.apache.flink.connector.file.table.stream.PartitionCommitInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.io.Serializable;
import java.util.Optional;

/** Utils to add and get some infos to StreamConfig. */
public class Utils {

  /**
   * Constructs a PartitionCommitInfo object for checkpoint completion notification.
   *
   * @param id checkpoint ID
   * @param subtaskIndex index of the subtask
   * @param numberOfSubtasks total number of subtasks
   * @param committed array of committed partition paths
   * @return Serializable PartitionCommitInfo object
   */
  public static Serializable constructCommitInfo(
      long id, int subtaskIndex, int numberOfSubtasks, String[] committed) {
    PartitionCommitInfo commitInfo =
        new PartitionCommitInfo(id, subtaskIndex, numberOfSubtasks, committed);
    return commitInfo;
  }

  /**
   * Extracts a GlutenOperator from a StreamConfig if it exists.
   *
   * @param streamConfig the stream configuration to check
   * @param userClassLoader the class loader to use for deserialization
   * @return Optional containing the GlutenOperator if found, empty otherwise
   */
  public static Optional<GlutenOperator> getGlutenOperator(
      StreamConfig streamConfig, ClassLoader userClassLoader) {
    StreamOperatorFactory operatorFactory = streamConfig.getStreamOperatorFactory(userClassLoader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      StreamOperator streamOperator = streamConfig.getStreamOperator(userClassLoader);
      if (streamOperator instanceof GlutenOperator) {
        return Optional.of((GlutenOperator) streamOperator);
      }
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return Optional.of(((GlutenOneInputOperatorFactory) operatorFactory).getOperator());
    }
    return Optional.empty();
  }
}
