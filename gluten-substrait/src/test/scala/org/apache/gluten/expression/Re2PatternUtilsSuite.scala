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

import org.scalatest.funsuite.AnyFunSuite

class Re2PatternUtilsSuite extends AnyFunSuite {

  private def tr(p: String): Option[String] = Re2PatternUtils.translateJavaPatternToRe2(p)

  test("plain pattern is unchanged") {
    assert(tr("[a-z]+") == Some("[a-z]+"))
  }

  test("unicode escape is translated and is idempotent") {
    assert(tr("\\u4e00") == Some("\\x{4e00}"))
    assert(tr("[\\u4e00-\\u9fa5]") == Some("[\\x{4e00}-\\x{9fa5}]"))
    assert(tr("\\x{4e00}") == Some("\\x{4e00}"))
    assert(tr("\\u41") == Some("\\u41"))
  }

  test("escaped backslash before u is not a unicode escape") {
    assert(tr("\\\\u0041") == Some("\\\\u0041"))
    assert(tr("\\\\\\u0041") == Some("\\\\\\x{0041}"))
  }

  test("surrogate pairs are combined into one code point") {
    assert(tr("\\uD83D\\uDE00") == Some("\\x{1F600}"))
    assert(tr("[\\uD83D\\uDE00-\\uD83D\\uDE4F]") == Some("[\\x{1F600}-\\x{1F64F}]"))
  }

  test("unpaired surrogates are unsupported") {
    assert(tr("\\uD83Dx").isEmpty)
    assert(tr("\\uDE00").isEmpty)
    assert(tr("\\uD83D\\u0041").isEmpty)
  }

  test("lookaround and backreferences are unsupported") {
    assert(tr("(?=x)").isEmpty)
    assert(tr("(?!x)").isEmpty)
    assert(tr("(?<=x)y").isEmpty)
    assert(tr("(?<!x)y").isEmpty)
    assert(tr("(a)\\1").isEmpty)
  }

  test("escaped or in-class constructs are not mistaken for unsupported ones") {
    assert(tr("\\\\1").isDefined)
    assert(tr("\\(?=x").isDefined)
    assert(tr("[(?=]").isDefined)
    assert(tr("[^]](?=)").isEmpty) // ']' right after '[^' is literal; the lookahead is real
    assert(tr("[]](?:a)").isDefined)
  }

  test("named groups and non-capturing groups are supported") {
    assert(tr("(?<name>a)") == Some("(?<name>a)"))
    assert(tr("(?:a)(?i)b") == Some("(?:a)(?i)b"))
  }

  test("any backslash-digit escape is a backreference, even inside a character class") {
    assert(tr("(a)\\1").isEmpty)
    assert(tr("(a)\\12").isEmpty)
    assert(tr("[\\1]").isEmpty)
    // RE2 reads this as an octal escape and would run it, while Java rejects it: must fall back.
    assert(tr("[\\12]").isEmpty)
    assert(tr("\\\\1").isDefined)
  }

  test("describePattern quotes, escapes control characters and truncates") {
    assert(Re2PatternUtils.describePattern("a(?=b)") == "'a(?=b)'")
    assert(Re2PatternUtils.describePattern("a\nb\tc") == "'a\\nb\\tc'")
    val long = Re2PatternUtils.describePattern("x" * 500)
    assert(long.length < 200 && long.contains("truncated, 500 chars total"))
  }
}
