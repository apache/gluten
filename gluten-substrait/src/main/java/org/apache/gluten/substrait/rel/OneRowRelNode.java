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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.utils.SubstraitUtil;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/** A zero-column, one-row virtual table for Spark's OneRowRelationExec. */
public class OneRowRelNode implements RelNode, Serializable {
  OneRowRelNode() {}

  @Override
  public Rel toProtobuf() {
    NamedStruct baseSchema =
        SubstraitUtil.createNameStructBuilder(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
            .build();

    ReadRel readRel =
        ReadRel.newBuilder()
            .setCommon(RelCommon.newBuilder().setDirect(RelCommon.Direct.newBuilder()).build())
            .setBaseSchema(baseSchema)
            .setVirtualTable(
                ReadRel.VirtualTable.newBuilder()
                    .addExpressions(Expression.Nested.Struct.getDefaultInstance())
                    .build())
            .build();
    return Rel.newBuilder().setRead(readRel).build();
  }

  @Override
  public List<RelNode> childNodes() {
    return Collections.emptyList();
  }
}
