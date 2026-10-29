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
package org.apache.spark.sql.gluten

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

class GlutenValidationSuite extends AnyFunSuite {
  private val plugin = "org.apache.gluten.GlutenPlugin"

  test("enabled flag alone does not establish plugin or rule installation") {
    intercept[IllegalArgumentException] {
      GlutenValidation.assertSettings("", true, None, true)
    }
    intercept[IllegalArgumentException] {
      GlutenValidation.assertSettings(plugin, true, None, false)
    }
  }

  test("both session and thread disable switches are checked") {
    intercept[IllegalArgumentException] {
      GlutenValidation.assertSettings(plugin, false, None, true)
    }
    intercept[IllegalArgumentException] {
      GlutenValidation.assertSettings(plugin, true, Some("false"), true)
    }
    intercept[IllegalArgumentException] {
      GlutenValidation.assertSettings(plugin, true, Some("invalid"), true)
    }
  }

  test("plugin lists and restored defaults are accepted") {
    GlutenValidation.assertSettings(s"other.Plugin, $plugin ", true, Some("true"), true)
    GlutenValidation.assertSettings(plugin, true, None, true)
  }

  test("test keys use the same newline normalization as the baseline gate") {
    assert(GlutenValidation.testKey("Suite", "a\r\nb ") == "Suite#a  b")
  }

  test("exemptions require explicit identities and reasons") {
    val path = Files.createTempFile("gluten-exemptions", ".txt")
    try {
      Files.write(path, "# comment\nSuite#test\tdeliberately vanilla\n".getBytes(UTF_8))
      assert(
        GlutenValidation.readExemptions(path) == Map("Suite#test" -> "deliberately vanilla"))
      Seq(
        "Suite#test",
        "Suite#test\t",
        "*#test\treason",
        "Suite#test\tone\nSuite#test\ttwo",
        "org.apache.spark.sql.gluten.GlutenActivationSuite#*\treason").foreach {
        text =>
          Files.write(path, text.getBytes(UTF_8))
          intercept[IllegalArgumentException] {
            GlutenValidation.readExemptions(path)
          }
      }
    } finally {
      Files.delete(path)
    }
  }
}
