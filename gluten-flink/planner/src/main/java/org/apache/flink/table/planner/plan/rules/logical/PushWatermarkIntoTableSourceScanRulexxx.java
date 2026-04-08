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
package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;

import org.apache.calcite.plan.RelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rule to push the {@link FlinkLogicalWatermarkAssigner} into the {@link
 * FlinkLogicalTableSourceScan}.
 */
public class PushWatermarkIntoTableSourceScanRulexxx
    extends PushWatermarkIntoTableSourceScanRuleBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(PushWatermarkIntoTableSourceScanRulexxx.class);
  public static final PushWatermarkIntoTableSourceScanRulexxx INSTANCE =
      new PushWatermarkIntoTableSourceScanRulexxx();

  public PushWatermarkIntoTableSourceScanRulexxx() {
    super(
        operand(
            FlinkLogicalWatermarkAssigner.class,
            operand(FlinkLogicalTableSourceScan.class, none())),
        "PushWatermarkIntoTableSourceScanRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LOG.info("PushWatermarkIntoTableSourceScanRule does not match xxxxx");
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {}
}
