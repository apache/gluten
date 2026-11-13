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

import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimeType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class LogicalTypeConverterTest {

  // L1: TIME(0) maps to the INTEGER milliseconds-of-day carrier.
  @Test
  public void testTimeZeroMapsToInteger() {
    Type type = LogicalTypeConverter.toVLType(new TimeType(0));
    assertThat(type).isInstanceOf(IntegerType.class);
  }

  // L2: TIME(3) maps to INTEGER — the carrier does not carry the precision.
  @Test
  public void testTimeThreeMapsToInteger() {
    Type type = LogicalTypeConverter.toVLType(new TimeType(3));
    assertThat(type).isInstanceOf(IntegerType.class);
  }

  // L3: TIME(6) maps to INTEGER.
  @Test
  public void testTimeSixMapsToInteger() {
    Type type = LogicalTypeConverter.toVLType(new TimeType(6));
    assertThat(type).isInstanceOf(IntegerType.class);
  }

  // L4: TIME(9) maps to INTEGER.
  @Test
  public void testTimeNineMapsToInteger() {
    Type type = LogicalTypeConverter.toVLType(new TimeType(9));
    assertThat(type).isInstanceOf(IntegerType.class);
  }

  // L5: the neighboring anchor types keep mapping to Timestamp — TIME is
  // not conflated with the timestamp family.
  @Test
  public void testTimestampAnchorsMapToTimestamp() {
    Type timestamp =
        LogicalTypeConverter.toVLType(new org.apache.flink.table.types.logical.TimestampType(3));
    assertThat(timestamp).isInstanceOf(TimestampType.class);

    Type timestampLtz = LogicalTypeConverter.toVLType(new LocalZonedTimestampType(3));
    assertThat(timestampLtz).isInstanceOf(TimestampType.class);
  }
}
