/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gluten.rexnode;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/** Extracts the window parameters of every TVF window spec into the velox node fields. */
public class WindowUtilsTest {
  private static WindowingStrategy timeStrategy(
      org.apache.flink.table.planner.plan.logical.WindowSpec spec) {
    return new TimeAttributeWindowingStrategy(
        spec, new TimestampType(true, TimestampKind.ROWTIME, 3), 1);
  }

  @Test
  public void testTumble() {
    final Tuple5<Long, Long, Long, Integer, Integer> out =
        WindowUtils.extractWindowParameters(timeStrategy(new TumblingWindowSpec(ofMillis(5000), null)));
    assertEquals(5000L, (long) out.f0);
    assertEquals(0L, (long) out.f1);
    assertEquals(0L, (long) out.f2);
    assertEquals(1, (int) out.f3);
    assertEquals(1, (int) out.f4);
  }

  @Test
  public void testHop() {
    final Tuple5<Long, Long, Long, Integer, Integer> out =
        WindowUtils.extractWindowParameters(
            timeStrategy(new HoppingWindowSpec(ofMillis(4000), ofMillis(2000), ofMillis(100))));
    assertEquals(4000L, (long) out.f0);
    assertEquals(2000L, (long) out.f1);
    assertEquals(100L, (long) out.f2);
    assertEquals(1, (int) out.f3);
    assertEquals(0, (int) out.f4);
  }

  @Test
  public void testSessionGapRidesSize() {
    // The SESSION translation maps the gap onto the size field; step and
    // offset stay unset - the velox operator reads gap = size.
    final Tuple5<Long, Long, Long, Integer, Integer> out =
        WindowUtils.extractWindowParameters(
            timeStrategy(new SessionWindowSpec(ofMillis(2000), new int[] {0})));
    assertEquals(2000L, (long) out.f0);
    assertEquals(0L, (long) out.f1);
    assertEquals(0L, (long) out.f2);
    assertEquals(1, (int) out.f3);
    assertEquals(2, (int) out.f4);
  }

  @Test
  public void testCumulativeDeferred() {
    // CUMULATE is deferred: the branch assigns the type code but extracts
    // no parameters, so the velox operator rejects the plan fail loud
    // through its L5 check before the size validation.
    final Tuple5<Long, Long, Long, Integer, Integer> out =
        WindowUtils.extractWindowParameters(
            timeStrategy(new CumulativeWindowSpec(ofMillis(4000), ofMillis(2000), null)));
    assertEquals(0L, (long) out.f0);
    assertEquals(0L, (long) out.f1);
    assertEquals(0L, (long) out.f2);
    assertEquals(1, (int) out.f3);
    assertEquals(3, (int) out.f4);
  }

  private static Duration ofMillis(long ms) {
    return Duration.ofMillis(ms);
  }
}
