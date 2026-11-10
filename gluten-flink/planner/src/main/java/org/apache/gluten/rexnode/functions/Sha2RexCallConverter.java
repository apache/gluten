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
 * Converts Flink SHA2 to the velox flinksql sha2 function.
 *
 * <p>A constant hashLength is validated at conversion time: Flink checks the constant length in the
 * generated operator's constructor ({@code StringCallGen#generateSha2} hands it to {@code
 * CodeGeneratorContext#addReusableSha2MessageDigest}, which throws "Unsupported algorithm." for
 * anything outside {224, 256, 384, 512}), so a job with a bad constant length fails regardless of
 * input — even on an empty table — while velox rejects per row and would silently succeed on zero
 * rows. Rejecting the constant form here keeps both stacks failing before any row is processed.
 *
 * <p>A NULL constant and a column reference cannot be checked at plan time and keep the existing
 * runtime rejection in velox, matching Flink's per-row behavior.
 */
public class Sha2RexCallConverter extends DefaultRexCallConverter {

  public Sha2RexCallConverter() {
    super("sha2");
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() == 2) {
      RexNode hashLength = callNode.getOperands().get(1);
      if (hashLength instanceof RexLiteral) {
        // getValue() covers every integer width exactly (Byte/Short/Integer/Long) and is
        // null for a NULL literal; longValue() avoids int-range truncation surprises.
        Object value = ((RexLiteral) hashLength).getValue();
        if (value instanceof Number) {
          long length = ((Number) value).longValue();
          if (length != 224 && length != 256 && length != 384 && length != 512) {
            return ValidationResult.failure(
                String.format(
                    "SHA2 with literal hashLength %d is not supported: supported lengths are"
                        + " 224, 256, 384 and 512 (Flink rejects an unsupported constant length"
                        + " in the generated operator's constructor as well)",
                    length));
          }
        }
      }
    }
    return ValidationResult.success();
  }
}
