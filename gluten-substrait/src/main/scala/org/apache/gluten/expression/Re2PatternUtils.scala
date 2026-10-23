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

import java.util.Locale

/**
 * Utilities for translating regex patterns from Java syntax to RE2 syntax. Used by
 * ExpressionConverter for any backend whose native engine is RE2 (Velox, Bolt); see
 * [[org.apache.gluten.backendsapi.SparkPlanExecApi#needsRe2PatternTranslation]].
 */
object Re2PatternUtils {

  private def isHex(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  /**
   * Renders a pattern for error messages: quoted, control characters escaped, and truncated so that
   * very long patterns do not bloat fallback reasons and logs.
   */
  def describePattern(pattern: String, maxLen: Int = 100): String = {
    val sb = new StringBuilder("'")
    var i = 0
    while (i < pattern.length && sb.length < maxLen) {
      pattern.charAt(i) match {
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case ch if ch < ' ' || ch == '\u007f' => sb.append("\\x%02X".format(ch.toInt))
        case ch => sb.append(ch)
      }
      i += 1
    }
    if (i < pattern.length) sb.append(s"...(truncated, ${pattern.length} chars total)")
    sb.append("'").toString
  }

  /** Returns the code unit of a backslash-u escape with 4 hex digits starting at `i`, or -1. */
  private def unicodeEscapeAt(p: String, i: Int): Int = {
    if (
      i + 6 <= p.length && p.charAt(i) == '\\' && p.charAt(i + 1) == 'u' &&
      (2 until 6).forall(k => isHex(p.charAt(i + k)))
    ) {
      Integer.parseInt(p.substring(i + 2, i + 6), 16)
    } else {
      -1
    }
  }

  /**
   * Attempts to translate a Java regex pattern to RE2-compatible syntax.
   *
   * Returns Some(translated) if all constructs have RE2 equivalents and the translation is
   * lossless. Returns None if the pattern contains constructs that RE2 cannot represent
   * (lookaheads, lookbehinds, backreferences) or an unpaired UTF-16 surrogate escape; the caller
   * should throw [[org.apache.gluten.exception.GlutenNotSupportException]] to trigger fallback.
   *
   * Translation rules:
   *   - backslash-u followed by 4 hex digits becomes `\x{XXXX}`; a high+low surrogate escape pair
   *     becomes a single `\x{code point}`.
   *   - Escaped characters (e.g. an escaped backslash followed by `u0041`, or an escaped
   *     parenthesis) are copied verbatim and never reinterpreted.
   *   - Lookaround markers are ignored inside character classes, where `(` is a literal.
   *   - Every backslash followed by a digit 1-9 is treated as a backreference and rejected (Java
   *     parses it as a group reference, whatever follows), including inside a character class,
   *     where Java itself raises an error.
   *   - Named groups `(?<name>...)` are supported by RE2 and are not rejected.
   */
  def translateJavaPatternToRe2(pattern: String): Option[String] = {
    val sb = new StringBuilder(pattern.length)
    val n = pattern.length
    var i = 0
    var inClass = false
    while (i < n) {
      val c = pattern.charAt(i)
      if (c == '\\') {
        val cp = unicodeEscapeAt(pattern, i)
        if (cp >= 0) {
          if (Character.isHighSurrogate(cp.toChar)) {
            val lo = unicodeEscapeAt(pattern, i + 6)
            if (lo < 0 || !Character.isLowSurrogate(lo.toChar)) return None
            val full = Character.toCodePoint(cp.toChar, lo.toChar)
            val hex = Integer.toHexString(full).toUpperCase(Locale.ROOT)
            sb.append("\\x{").append(hex).append("}")
            i += 12
          } else if (Character.isLowSurrogate(cp.toChar)) {
            return None
          } else {
            sb.append("\\x{").append(pattern.substring(i + 2, i + 6)).append("}")
            i += 6
          }
        } else if (i + 1 < n) {
          val next = pattern.charAt(i + 1)
          // Backreference. Any 1-9 is rejected, including inside a character class, where Java
          // itself raises an error; falling back lets Spark report that same error.
          if (next >= '1' && next <= '9') return None
          sb.append(c).append(next)
          i += 2
        } else {
          sb.append(c)
          i += 1
        }
      } else if (inClass) {
        if (c == ']') inClass = false
        sb.append(c)
        i += 1
      } else if (c == '[') {
        inClass = true
        sb.append(c)
        i += 1
        // A leading '^' and a ']' right after '[' (or '[^') are literal members of the class.
        if (i < n && pattern.charAt(i) == '^') {
          sb.append('^')
          i += 1
        }
        if (i < n && pattern.charAt(i) == ']') {
          sb.append(']')
          i += 1
        }
      } else {
        if (c == '(' && i + 2 < n && pattern.charAt(i + 1) == '?') {
          val k = pattern.charAt(i + 2)
          val lookbehind = k == '<' && i + 3 < n && "=!".indexOf(pattern.charAt(i + 3)) >= 0
          if (k == '=' || k == '!' || lookbehind) return None
        }
        sb.append(c)
        i += 1
      }
    }
    Some(sb.toString())
  }
}
