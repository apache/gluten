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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RexCallConverterFactory {

  /**
   * Better to new Converter for each call. Reusing an object can easily introduce potential issues.
   *
   * <p>A single operator (e.g., '+') may map to multiple converters, such as arithmetic addition
   * and timestamp interval addition, which require distinct implementations. We need to find the
   * only suitable converter for the given RexCall node.
   */
  private static Map<String, List<RexCallConverterBuilder>> converters =
      Map.ofEntries(
          Map.entry(
              ">",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("greaterthan"),
                  () -> new StringCompareRexCallConverter("greaterthan"),
                  () -> new StringNumberCompareRexCallConverter("greaterthan"),
                  () -> new IntervalCompareRexCallConverter("interval_greaterthan"),
                  () -> new DecimalArithmeticOperatorRexCallConverters("greaterthan", true))),
          Map.entry(
              "<",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("lessthan"),
                  () -> new StringCompareRexCallConverter("lessthan"),
                  () -> new StringNumberCompareRexCallConverter("lessthan"),
                  () -> new IntervalCompareRexCallConverter("interval_lessthan"),
                  () -> new DecimalArithmeticOperatorRexCallConverters("lessthan", true))),
          Map.entry(
              "=",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("equalto"),
                  () -> new StringCompareRexCallConverter("equalto"),
                  () -> new StringNumberCompareRexCallConverter("equalto"),
                  () -> new IntervalCompareRexCallConverter("interval_equalto"))),
          Map.entry(
              "<>",
              Arrays.asList(
                  () -> new DecimalArithmeticOperatorRexCallConverters("decimal_notequalto"),
                  () -> new IntervalCompareRexCallConverter("interval_notequalto"))),
          Map.entry(
              "/", Arrays.asList(() -> new DecimalArithmeticOperatorRexCallConverters("divide"))),
          Map.entry(
              "*",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("multiply"),
                  () -> new DecimalArithmeticOperatorRexCallConverters("multiply"),
                  () -> new IntervalMultiplyRexCallConverter())),
          Map.entry(
              "-",
              Arrays.asList(
                  () -> new UnaryMinusRexCallConverter("negate"),
                  () -> new SubtractRexCallConverter(),
                  () -> new DecimalArithmeticOperatorRexCallConverters("subtract"),
                  () -> new IntervalArithmeticRexCallConverter("minus", false))),
          Map.entry(
              "+",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("add"),
                  () -> new DecimalArithmeticOperatorRexCallConverters("add"),
                  () -> new IntervalArithmeticRexCallConverter("plus", true))),
          Map.entry("MOD", Arrays.asList(() -> new ModRexCallConverter())),
          Map.entry("Reinterpret", Arrays.asList(() -> new ReinterpretRexCallConverter())),
          Map.entry("CAST", Arrays.asList(() -> new DefaultRexCallConverter("cast"))),
          Map.entry("CASE", Arrays.asList(() -> new DefaultRexCallConverter("if"))),
          Map.entry("AND", Arrays.asList(() -> new DefaultRexCallConverter("and"))),
          Map.entry("SPLIT_INDEX", Arrays.asList(() -> new SplitIndexRexCallConverter())),
          Map.entry("SEARCH", Arrays.asList(() -> new SearchRexCallConverter())),
          Map.entry("DATE_FORMAT", Arrays.asList(() -> new DefaultRexCallConverter("date_format"))),
          Map.entry("FROM_UNIXTIME", Arrays.asList(() -> new FromUnixtimeRexCallConverter())),
          Map.entry("UNIX_TIMESTAMP", Arrays.asList(() -> new UnixTimestampRexCallConverter())),
          Map.entry(
              ">=",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("greaterthanorequal"),
                  () -> new StringCompareRexCallConverter("greaterthanorequal"),
                  () -> new StringNumberCompareRexCallConverter("greaterthanorequal"),
                  () -> new TimestampIntervalRexCallConverter("greaterthanorequal"),
                  () -> new IntervalCompareRexCallConverter("interval_greaterthanorequal"))),
          Map.entry(
              "<=",
              Arrays.asList(
                  () -> new BasicArithmeticOperatorRexCallConverter("lessthanorequal"),
                  () -> new StringCompareRexCallConverter("lessthanorequal"),
                  () -> new StringNumberCompareRexCallConverter("lessthanorequal"),
                  () -> new TimestampIntervalRexCallConverter("lessthanorequal"),
                  () -> new IntervalCompareRexCallConverter("interval_lessthanorequal"))),
          Map.entry("PROCTIME", Arrays.asList(() -> new DefaultRexCallConverter("unix_timestamp"))),
          Map.entry("OR", Arrays.asList(() -> new DefaultRexCallConverter("or"))),
          Map.entry("IS NOT NULL", Arrays.asList(() -> new DefaultRexCallConverter("isnotnull"))),
          Map.entry("IS NULL", Arrays.asList(() -> new DefaultRexCallConverter("isnull"))),
          Map.entry(
              "REGEXP_EXTRACT", Arrays.asList(() -> new DefaultRexCallConverter("regexp_extract"))),
          // Direct name mapping: both arities are covered by the flinksql to_date signatures.
          Map.entry("TO_DATE", Arrays.asList(() -> new DefaultRexCallConverter("to_date"))),
          // Direct name mapping: both arities are covered by the flinksql to_timestamp
          // signatures; the returned TIMESTAMP(3) maps to the velox TimestampType.
          Map.entry(
              "TO_TIMESTAMP", Arrays.asList(() -> new DefaultRexCallConverter("to_timestamp"))),
          // Direct name mapping: the only SQL form is (numeric, precision) — the first
          // operand's type is routed by the velox signature set, the returned
          // TIMESTAMP_LTZ(3) maps to the velox TimestampType.
          Map.entry(
              "TO_TIMESTAMP_LTZ",
              Arrays.asList(() -> new DefaultRexCallConverter("to_timestamp_ltz"))),
          Map.entry("LOWER", Arrays.asList(() -> new DefaultRexCallConverter("lower"))),
          // Direct name mapping: the SHA-2 family. The fixed-length spellings
          // are the sha2 core with the length bound on the velox side; the
          // hashLength operand is routed by the velox sha2 signature set.
          // SHA2 itself carries a converter-side validation of a constant
          // hashLength (see Sha2RexCallConverter).
          Map.entry("SHA2", Arrays.asList(() -> new Sha2RexCallConverter())),
          Map.entry("SHA224", Arrays.asList(() -> new DefaultRexCallConverter("sha224"))),
          Map.entry("SHA256", Arrays.asList(() -> new DefaultRexCallConverter("sha256"))),
          Map.entry("SHA384", Arrays.asList(() -> new DefaultRexCallConverter("sha384"))),
          Map.entry("SHA512", Arrays.asList(() -> new DefaultRexCallConverter("sha512"))),
          Map.entry("MD5", Arrays.asList(() -> new DefaultRexCallConverter("md5"))),
          Map.entry("SHA1", Arrays.asList(() -> new DefaultRexCallConverter("sha1"))),
          Map.entry("count_char", Arrays.asList(() -> new DefaultRexCallConverter("count_char"))),
          Map.entry("EXTRACT", Arrays.asList(() -> new DefaultRexCallConverter("extract"))),
          Map.entry("IS TRUE", Arrays.asList(() -> new DefaultRexCallConverter("is_true"))),
          Map.entry("IS FALSE", Arrays.asList(() -> new DefaultRexCallConverter("is_false"))),
          Map.entry("IS NOT TRUE", Arrays.asList(() -> new DefaultRexCallConverter("is_not_true"))),
          Map.entry(
              "IS NOT FALSE", Arrays.asList(() -> new DefaultRexCallConverter("is_not_false"))),
          Map.entry("NOT", Arrays.asList(() -> new DefaultRexCallConverter("not"))),
          Map.entry(
              "LOCALTIMESTAMP", Arrays.asList(() -> new DefaultRexCallConverter("localtimestamp"))),
          // LOCALTIME maps by name to the flinksql function returning the
          // INTEGER carrier (milliseconds of day in the session timezone);
          // the result type goes through toType(TIME(0)) -> IntegerType.
          Map.entry("LOCALTIME", Arrays.asList(() -> new DefaultRexCallConverter("localtime"))),
          Map.entry(
              "CURRENT_TIMESTAMP",
              Arrays.asList(() -> new DefaultRexCallConverter("current_timestamp"))),
          Map.entry("NOW", Arrays.asList(() -> new DefaultRexCallConverter("now"))),
          Map.entry("CONCAT", Arrays.asList(() -> new DefaultRexCallConverter("concat"))),
          Map.entry("CONCAT_WS", Arrays.asList(() -> new DefaultRexCallConverter("concat_ws"))),
          // A literal negative length is rejected by SubstringRexCallConverter
          // (stopgap until a Flink-semantics substring in the velox flinksql
          // function set returns NULL): Flink returns NULL while velox
          // substring returns an empty string. Start semantics (0 counts as 1,
          // negative counts from the end) already match Flink.
          Map.entry("SUBSTRING", Arrays.asList(() -> new SubstringRexCallConverter())),
          // Offload targets implemented in velox's flinksql namespace. ABS/
          // FLOOR/CEIL/ROUND arrive upper-cased from the operator table;
          // unary minus arrives as a one-operand "-" (see above).
          Map.entry("ABS", Arrays.asList(() -> new DefaultRexCallConverter("abs"))),
          Map.entry("FLOOR", Arrays.asList(() -> new NumericOnlyRexCallConverter("floor"))),
          Map.entry("CEIL", Arrays.asList(() -> new NumericOnlyRexCallConverter("ceil"))),
          Map.entry("ROUND", Arrays.asList(() -> new DecimalPostCastRexCallConverter("round"))),
          // Offload targets implemented in velox's flinksql namespace;
          // the names arrive upper-cased from the operator table.
          Map.entry(
              "CURRENT_DATE", Arrays.asList(() -> new DefaultRexCallConverter("current_date"))),
          Map.entry(
              "CURRENT_TIME", Arrays.asList(() -> new DefaultRexCallConverter("current_time"))),
          Map.entry("IFNULL", Arrays.asList(() -> new ScalarOnlyRexCallConverter("ifnull"))),
          // TRIM is normalized by Calcite to (flag, seek, str); the converter
          // maps the flag to ltrim/rtrim/trim and reorders to (str, seek).
          Map.entry("TRIM", Arrays.asList(() -> new TrimRexCallConverter())),
          // LTRIM/RTRIM keep the Flink argument order (str[, seek]); the
          // converter routes a two-argument call by the seek constancy:
          // a constant seek takes the flinksql character-set signatures, a
          // runtime-sourced seek takes the *_runtime ones (Flink's
          // isSpaceString degrades a seek starting with a space to
          // space-only trimming for segment-backed strings).
          Map.entry("LTRIM", Arrays.asList(() -> new TrimSeekRexCallConverter("ltrim"))),
          Map.entry("RTRIM", Arrays.asList(() -> new TrimSeekRexCallConverter("rtrim"))),
          Map.entry("LPAD", Arrays.asList(() -> new DefaultRexCallConverter("lpad"))),
          Map.entry("RPAD", Arrays.asList(() -> new DefaultRexCallConverter("rpad"))),
          Map.entry("COALESCE", Arrays.asList(() -> new DefaultRexCallConverter("coalesce"))),
          Map.entry("JSON_VALUE", Arrays.asList(() -> new JsonValueRexCallConverter())));

  public static RexCallConverter getConverter(RexCall callNode, RexConversionContext context) {
    String operatorName = callNode.getOperator().getName();
    List<RexCallConverterBuilder> builders = converters.get(operatorName);
    if (builders == null) {
      throw new RuntimeException("Function not supported: " + operatorName);
    }

    List<String> failureMessages = new ArrayList<>();
    List<RexCallConverter> converterList =
        builders.stream()
            .map(RexCallConverterBuilder::build)
            .filter(
                c -> {
                  ValidationResult validationResult;
                  try {
                    validationResult = c.isSuitable(callNode, context);
                  } catch (RuntimeException e) {
                    // A converter blowing up while probing operand types
                    // (e.g. toType on an unmapped interval) counts as not
                    // suitable so the decision stays a planning-time
                    // rejection instead of a hard failure.
                    failureMessages.add(c.getClass().getName() + ": " + e.getMessage());
                    return false;
                  }
                  if (!validationResult.isOk()) {
                    failureMessages.add(
                        c.getClass().getName() + ": " + validationResult.getMessage());
                    return false;
                  } else {
                    return true;
                  }
                })
            .collect(Collectors.toList());

    if (converterList.size() > 1) {
      String converterClasses =
          converterList.stream()
              .map(converter -> converter.getClass().getName())
              .collect(Collectors.joining(", "));
      String message =
          String.format(
              "Multiple converters found for: %s. Converters: %s.", operatorName, converterClasses);
      throw new RuntimeException(message);
    } else if (converterList.isEmpty()) {
      String message =
          String.format(
              "No suitable converter found for: %s. Reason:\n%s",
              operatorName, String.join("\n", failureMessages));
      throw new RuntimeException(message);
    }

    return converterList.get(0);
  }

  private interface RexCallConverterBuilder {
    RexCallConverter build();
  }
}
