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
package org.apache.gluten.rexnode.functions;

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.type.VarCharType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/**
 * Converts Flink UNIX_TIMESTAMP to the velox flinksql unix_timestamp function.
 *
 * <p>Flink SQL accepts zero, one or two string operands; every operand must be a string (the
 * timestamp overloads belong to the Table API only). The result type is forced to BIGINT by
 * DefaultRexCallConverter.
 */
public class UnixTimestampRexCallConverter extends DefaultRexCallConverter {

  public UnixTimestampRexCallConverter() {
    super("unix_timestamp");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() > 2) {
      return ValidationResult.failure(
          String.format(
              "UNIX_TIMESTAMP expects at most 2 operands, but found: %s",
              getFunctionProtoTypeName(callNode)));
    }
    for (RexNode operand : callNode.getOperands()) {
      if (!(RexNodeConverter.toType(operand.getType()) instanceof VarCharType)) {
        return ValidationResult.failure(
            String.format(
                "UNIX_TIMESTAMP operands must be strings, but found: %s",
                getFunctionProtoTypeName(callNode)));
      }
    }
    return ValidationResult.success();
  }
}
