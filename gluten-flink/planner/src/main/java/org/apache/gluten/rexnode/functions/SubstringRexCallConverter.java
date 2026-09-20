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
import org.apache.gluten.rexnode.ValidationResult;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * Converts Flink SUBSTRING to the velox substring function (1-based; the SQL {@code SUBSTRING(x
 * FROM n FOR m)} form desugars to the same call).
 *
 * <p>Start semantics already match: Flink's runtime ({@code BinaryStringDataUtil#substringSQL})
 * treats a start of 0 like 1 and counts a negative start from the string end — the same rules velox
 * substring implements — so starts convert as-is. Only a start more negative than the string length
 * diverges (Flink returns an empty string, velox returns a prefix from the head), but whether it
 * triggers depends on row data and cannot be checked at plan time.
 *
 * <p>A literal negative length is rejected: Flink returns NULL while velox returns an empty string.
 *
 * <p>TODO: the rejection is a stopgap. The proper fix is a Flink-semantics substring function in
 * the velox flinksql function set that returns NULL for a negative length (and an empty string for
 * an over-long negative start). A non-literal length cannot be checked at plan time and is still
 * converted as-is; both gaps close once SUBSTRING maps to that function and this rejection is
 * lifted.
 */
public class SubstringRexCallConverter extends DefaultRexCallConverter {

  public SubstringRexCallConverter() {
    super("substring");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() > 2) {
      RexNode length = callNode.getOperands().get(2);
      if (length instanceof RexLiteral) {
        Integer lengthValue = ((RexLiteral) length).getValueAs(Integer.class);
        if (lengthValue != null && lengthValue < 0) {
          return ValidationResult.failure(
              String.format(
                  "SUBSTRING with literal length %d is not supported: Flink returns NULL for a"
                      + " negative length while velox substring returns an empty string",
                  lengthValue));
        }
      }
    }
    return ValidationResult.success();
  }
}
