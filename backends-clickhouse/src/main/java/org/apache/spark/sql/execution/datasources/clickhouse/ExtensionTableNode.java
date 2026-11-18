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
package org.apache.spark.sql.execution.datasources.clickhouse;

import org.apache.gluten.substrait.rel.SplitInfo;
import org.apache.gluten.utils.SubstraitUtil;

import com.google.protobuf.StringValue;
import io.substrait.proto.ReadRel;

import java.util.ArrayList;
import java.util.List;

public class ExtensionTableNode implements SplitInfo {

  private final List<String> preferredLocations = new ArrayList<>();
  private final String serializerResult;
  private final scala.collection.Seq<String> pathList;

  ExtensionTableNode(
      List<String> preferredLocations,
      String serializerResult,
      scala.collection.Seq<String> pathList) {
    this.preferredLocations.addAll(preferredLocations);
    this.serializerResult = serializerResult;
    this.pathList = pathList;
  }

  @Override
  public List<String> preferredLocations() {
    return this.preferredLocations;
  }

  @Override
  public ReadRel.ExtensionTable toProtobuf() {
    return toProtobuf(serializerResult);
  }

  public scala.collection.Seq<String> getPartList() {
    return pathList;
  }

  public String getExtensionTableStr() {
    return serializerResult;
  }

  public static ReadRel.ExtensionTable toProtobuf(String result) {
    return SubstraitUtil.packExtensionTable(StringValue.newBuilder().setValue(result).build());
  }
}
