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

  private def tr(p: String): Either[String, String] = Re2PatternUtils.translateJavaPatternToRe2(p)

  private def reason(p: String): String = tr(p).left.getOrElse(fail(s"expected rejection of $p"))

  test("plain pattern is unchanged") {
    assert(tr("[a-z]+") == Right("[a-z]+"))
  }

  test("unicode escape is translated and is idempotent") {
    assert(tr("\\u4e00") == Right("\\x{4e00}"))
    assert(tr("[\\u4e00-\\u9fa5]") == Right("[\\x{4e00}-\\x{9fa5}]"))
    assert(tr("\\x{4e00}") == Right("\\x{4e00}"))
    assert(tr("\\u41") == Right("\\u41"))
  }

  test("escaped backslash before u is not a unicode escape") {
    assert(tr("\\\\u0041") == Right("\\\\u0041"))
    assert(tr("\\\\\\u0041") == Right("\\\\\\x{0041}"))
  }

  test("surrogate pairs are combined into one code point") {
    assert(tr("\\uD83D\\uDE00") == Right("\\x{1F600}"))
    assert(tr("[\\uD83D\\uDE00-\\uD83D\\uDE4F]") == Right("[\\x{1F600}-\\x{1F64F}]"))
  }

  test("unpaired surrogates are unsupported") {
    assert(tr("\\uD83Dx").isLeft)
    assert(tr("\\uDE00").isLeft)
    assert(tr("\\uD83D\\u0041").isLeft)
  }

  test("lookaround and backreferences are unsupported") {
    assert(tr("(?=x)").isLeft)
    assert(tr("(?!x)").isLeft)
    assert(tr("(?<=x)y").isLeft)
    assert(tr("(?<!x)y").isLeft)
    assert(tr("(a)\\1").isLeft)
  }

  test("escaped or in-class constructs are not mistaken for unsupported ones") {
    assert(tr("\\\\1").isRight)
    assert(tr("\\(?=x").isRight)
    assert(tr("[(?=]").isRight)
    assert(tr("[^]](?=)").isLeft) // ']' right after '[^' is literal; the lookahead is real
    assert(tr("[]](?:a)").isRight)
  }

  test("named groups and non-capturing groups are supported") {
    assert(tr("(?<name>a)") == Right("(?<name>a)"))
    assert(tr("(?:a)(?i)b") == Right("(?:a)(?i)b"))
  }

  test("any backslash-digit escape is a backreference, even inside a character class") {
    assert(tr("(a)\\1").isLeft)
    assert(tr("(a)\\12").isLeft)
    assert(tr("[\\1]").isLeft)
    // RE2 reads this as an octal escape and would run it, while Java rejects it: must fall back.
    assert(tr("[\\12]").isLeft)
    assert(tr("\\\\1").isRight)
  }

  test("rejections name the offending construct") {
    assert(reason("(?=x)").contains("lookahead/lookbehind"))
    assert(reason("(?<!x)y").contains("lookahead/lookbehind"))
    assert(reason("(a)\\1").contains("backreference \\1"))
    assert(reason("\\uD83Dx").contains("unpaired surrogate escape \\uD83D"))
    assert(reason("\\uDE00").contains("unpaired surrogate escape \\uDE00"))
    assert(reason("a\\0b").contains("octal escape"))
  }

  test("octal escape is unsupported, but an escaped backslash before 0 is not") {
    assert(tr("\\0").isLeft)
    assert(tr("\\0377").isLeft)
    assert(tr("[\\0]").isLeft)
    assert(tr("\\\\0") == Right("\\\\0"))
  }

  test("quoted sections are copied verbatim") {
    // Content between \Q and \E is literal text in Java and RE2: never translated or rejected.
    assert(tr("\\Q\\u0041\\E") == Right("\\Q\\u0041\\E"))
    assert(tr("\\Q(?=\\E") == Right("\\Q(?=\\E"))
    assert(tr("\\Q\\1\\E") == Right("\\Q\\1\\E"))
    assert(tr("\\Q\\0\\E") == Right("\\Q\\0\\E"))
    // Regex syntax before and after the quoted section is still translated and checked.
    assert(tr("\\u0041\\Q\\u0041\\E\\u0042") == Right("\\x{0041}\\Q\\u0041\\E\\x{0042}"))
    assert(tr("\\Qa\\E(?=x)").isLeft)
    assert(tr("\\Qa\\E\\1").isLeft)
  }

  test("quoted section edge cases") {
    // Unterminated: quoted to the end of the pattern.
    assert(tr("a\\Q\\u0041(?=") == Right("a\\Q\\u0041(?="))
    // Ends at the first \E.
    assert(tr("\\Qa\\Eb\\u0041") == Right("\\Qa\\Eb\\x{0041}"))
    // Empty quote.
    assert(tr("\\Q\\E\\u0041") == Right("\\Q\\E\\x{0041}"))
    // An escaped backslash before Q is not a quote start.
    assert(tr("\\\\Q\\u0041") == Right("\\\\Q\\x{0041}"))
    // A stray \E with no \Q is passed through.
    assert(tr("a\\Eb") == Right("a\\Eb"))
    // A quote inside a character class does not end the class early.
    assert(tr("[\\Q]\\E](?=x)").isLeft)
  }

  test("describePattern quotes, escapes control characters and truncates") {
    assert(Re2PatternUtils.describePattern("a(?=b)") == "'a(?=b)'")
    assert(Re2PatternUtils.describePattern("a\nb\tc") == "'a\\nb\\tc'")
    val long = Re2PatternUtils.describePattern("x" * 500)
    assert(long.length < 200 && long.contains("truncated, 500 chars total"))
  }
}
