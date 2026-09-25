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
package org.apache.gluten.expression

/**
 * Utilities for translating regex patterns from Java syntax to RE2 syntax. Called by
 * ExpressionConverter when building the native Substrait plan.
 */
object VeloxRegexUtils {

  /**
   * Attempts to translate a Java regex pattern to RE2-compatible syntax.
   *
   * Returns Some(translated) if all constructs have RE2 equivalents and the translation is
   * lossless. Returns None if the pattern contains constructs that RE2 cannot represent
   * (lookaheads, lookbehinds, backreferences) — the caller should throw GlutenNotSupportException
   * to trigger fallback.
   */
  def translateJavaPatternToRe2(pattern: String): Option[String] = {
    if (containsUnsupportedConstruct(pattern)) None
    else Some(translateUnicodeEscapes(pattern))
  }

  /** Returns true if the pattern uses a construct that has no RE2 equivalent. */
  def containsUnsupportedConstruct(pattern: String): Boolean = {
    // Match lookaheads (?= and (?!, and lookbehinds (?<= and (?<!.
    // Deliberately excludes (?<name> named capture groups, which RE2 supports.
    val lookaheadOrBehind = """\(\?(?:[=!]|<[=!])""".r // (?=  (?!  (?<=  (?<!
    val backreference = """\\[1-9]""".r // \1 through \9
    lookaheadOrBehind.findFirstIn(pattern).isDefined ||
    backreference.findFirstIn(pattern).isDefined
  }

  /**
   * Converts Java \uXXXX Unicode escapes (exactly 4 hex digits) to RE2's \x{XXXX} form. Any \u not
   * followed by exactly 4 hex digits is left unchanged. Already-translated \x{XXXX} sequences are
   * not reprocessed (idempotent).
   */
  def translateUnicodeEscapes(pattern: String): String = {
    val sb = new StringBuilder(pattern.length)
    var i = 0
    while (i < pattern.length) {
      if (
        i + 6 <= pattern.length &&
        pattern.charAt(i) == '\\' &&
        pattern.charAt(i + 1) == 'u' &&
        pattern.substring(i + 2, i + 6).forall("0123456789abcdefABCDEF".contains(_))
      ) {
        sb.append("\\x{").append(pattern.substring(i + 2, i + 6)).append("}")
        i += 6
      } else {
        sb.append(pattern.charAt(i))
        i += 1
      }
    }
    sb.toString()
  }
}
