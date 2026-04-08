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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;

import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Rule to push the {@link FlinkLogicalWatermarkAssigner} across the {@link FlinkLogicalCalc} to the
 * {@link FlinkLogicalTableSourceScan}. The rule will first look for the computed column in the
 * {@link FlinkLogicalCalc} and then translate the watermark expression and the computed column into
 * a {@link WatermarkStrategy}. With the new scan the rule will build a new {@link
 * FlinkLogicalCalc}.
 */
public class PushWatermarkIntoTableSourceScanAcrossCalcRulexxx
    extends PushWatermarkIntoTableSourceScanRuleBase {
  public static final PushWatermarkIntoTableSourceScanAcrossCalcRulexxx INSTANCE =
      new PushWatermarkIntoTableSourceScanAcrossCalcRulexxx();

  public PushWatermarkIntoTableSourceScanAcrossCalcRulexxx() {
    super(
        operand(
            FlinkLogicalWatermarkAssigner.class,
            operand(FlinkLogicalCalc.class, operand(FlinkLogicalTableSourceScan.class, none()))),
        "PushWatermarkIntoFlinkTableSourceScanAcrossCalcRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {}
}
