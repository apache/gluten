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
package org.apache.gluten.rexnode;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.planner.plan.logical.CumulativeWindowSpec;
import org.apache.flink.table.planner.plan.logical.HoppingWindowSpec;
import org.apache.flink.table.planner.plan.logical.SessionWindowSpec;
import org.apache.flink.table.planner.plan.logical.SliceAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.logical.WindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.runtime.groupwindow.WindowEnd;
import org.apache.flink.table.runtime.groupwindow.WindowStart;
import org.apache.flink.table.types.logical.RowType;

import java.time.Duration;
import java.util.List;

/** Utility to store some useful functions. */
public class WindowUtils {

  // Get names for project node.
  public static Tuple5<Long, Long, Long, Integer, Integer> extractWindowParameters(
      WindowingStrategy windowing) {
    long size = 0;
    long slide = 0;
    long offset = 0;
    int rowtimeIndex = -1;
    int windowType = -1;
    WindowSpec windowSpec = windowing.getWindow();
    if (windowSpec instanceof HoppingWindowSpec) {
      size = ((HoppingWindowSpec) windowSpec).getSize().toMillis();
      slide = ((HoppingWindowSpec) windowSpec).getSlide().toMillis();
      if (size % slide != 0) {
        throw new RuntimeException(
            String.format(
                "HOP table function based aggregate requires size must be an "
                    + "integral multiple of slide, but got size %s ms and slide %s ms",
                size, slide));
      }
      Duration windowOffset = ((HoppingWindowSpec) windowSpec).getOffset();
      if (windowOffset != null) {
        offset = windowOffset.toMillis();
      }
      windowType = 0;
    } else if (windowSpec instanceof TumblingWindowSpec) {
      size = ((TumblingWindowSpec) windowSpec).getSize().toMillis();
      Duration windowOffset = ((TumblingWindowSpec) windowSpec).getOffset();
      if (windowOffset != null) {
        offset = windowOffset.toMillis();
      }
      windowType = 1;
    } else if (windowSpec instanceof CumulativeWindowSpec) {
      windowType = 2;
    } else if (windowSpec instanceof SessionWindowSpec) {
      windowType = 3;
    } else {
      throw new RuntimeException("Not support window spec " + windowSpec);
    }
    if (windowing instanceof TimeAttributeWindowingStrategy) {
      if (windowing.isRowtime()) {
        rowtimeIndex = ((TimeAttributeWindowingStrategy) windowing).getTimeAttributeIndex();
      }
    } else if (windowing instanceof WindowAttachedWindowingStrategy) {
      rowtimeIndex = ((WindowAttachedWindowingStrategy) windowing).getWindowEnd();
    } else if (windowing instanceof SliceAttachedWindowingStrategy) {
      rowtimeIndex = ((SliceAttachedWindowingStrategy) windowing).getSliceEnd();
    } else {
      throw new RuntimeException("Not support window strategy " + windowing);
    }
    return new Tuple5<Long, Long, Long, Integer, Integer>(
        size, slide, offset, rowtimeIndex, windowType);
  }

  public static Tuple2<Integer, Integer> getWindowStartAndEndIndexes(
      NamedWindowProperty[] props, RowType outputType) {
    int windowStartIndex = -1, windowEndIndex = -1;
    List<String> outputNames = outputType.getFieldNames();
    for (NamedWindowProperty prop : props) {
      if (prop.getProperty() instanceof WindowStart) {
        windowStartIndex = outputNames.indexOf(prop.getName());
      } else if (prop.getProperty() instanceof WindowEnd) {
        windowEndIndex = outputNames.indexOf(prop.getName());
      }
    }
    return new Tuple2<Integer, Integer>(windowStartIndex, windowEndIndex);
  }
}
