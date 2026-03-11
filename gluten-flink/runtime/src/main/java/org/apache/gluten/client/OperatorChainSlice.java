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
package org.apache.gluten.client;

import org.apache.flink.streaming.api.graph.StreamConfig;

import java.util.ArrayList;
import java.util.List;

// Split operator chain into slices for offloading
// In the same slice, operators are all could offload or not.
public class OperatorChainSlice {
  // upstream slice indices
  private List<Integer> inputs;
  // downstream slice indices
  private List<Integer> outputs;
  private List<StreamConfig> operatorConfigs;
  private Integer id;
  private Boolean offloadable = false;

  public OperatorChainSlice(Integer id) {
    inputs = new ArrayList<>();
    outputs = new ArrayList<>();
    operatorConfigs = new ArrayList<>();
    this.id = id;
  }

  public Integer id() {
    return id;
  }

  public List<Integer> getInputs() {
    return inputs;
  }

  public List<Integer> getOutputs() {
    return outputs;
  }

  public List<StreamConfig> getOperatorConfigs() {
    return operatorConfigs;
  }

  public Boolean isOffloadable() {
    return offloadable;
  }

  public void setOffloadable(Boolean offloadable) {
    this.offloadable = offloadable;
  }
}
