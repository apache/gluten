#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Install CI-only fixture validation into the prepared upstream Delta checkout."""

import argparse
from pathlib import Path
import re
import shutil
import sys

SUPPORT = Path(__file__).resolve().parent
SOURCES = SUPPORT.parents[3] / "gluten-ut/src-delta-validation"
PACKAGE = "org.apache.spark.sql.gluten"
CHECKED = PACKAGE + ".GlutenValidation"
SHARED_IMPORT = (
    "import " + PACKAGE + ".{GlutenSharedSparkSession => SharedSparkSession}"
)
FOREIGN_HEADER_DIRECTIVE = "// scalastyle:off header.matches\n"


def replace_once(text, old, new, description):
    if new in text:
        return text
    if text.count(old) != 1:
        raise ValueError("{}: expected exactly one {!r}".format(description, old))
    return text.replace(old, new)


def shared_imports(text):
    """Alias the fixture import, not type names inside test strings or comments."""
    text = re.sub(
        r"(?m)^import org\.apache\.spark\.sql\.test\.SharedSparkSession[ \t]*$",
        SHARED_IMPORT,
        text,
    )

    def grouped(match):
        names = [name.strip() for name in match.group(1).split(",")]
        if "SharedSparkSession" not in names:
            return match.group(0)
        names.remove("SharedSparkSession")
        original = (
            "import org.apache.spark.sql.test.{" + ", ".join(names) + "}\n"
            if names
            else ""
        )
        return original + SHARED_IMPORT

    return re.sub(
        r"(?m)^import org\.apache\.spark\.sql\.test\.\{([^}]+)\}", grouped, text
    )


def add_import(text, name):
    statement = "import " + name
    if statement in text:
        return text
    pattern = r"(?m)^(package [\w.]+[ \t]*\r?\n)"
    text, count = re.subn(
        pattern, lambda match: match[0] + "\n" + statement + "\n", text
    )
    if count != 1:
        raise ValueError("Expected one package declaration when adding " + name)
    return text


def add_hook(text, class_name):
    pattern = r"((?:class|trait)\s+" + re.escape(class_name) + r"\b[^{}]*)(\{)"
    matches = list(re.finditer(pattern, text))
    if len(matches) != 1:
        raise ValueError("Expected one declaration of " + class_name)
    header = re.sub(
        r"\s+with\s+" + re.escape(PACKAGE + ".GlutenCheckedTest"),
        "",
        matches[0][1],
    )
    suffix = text[matches[0].end() :]
    return (
        text[: matches[0].start()]
        + (header.rstrip() + "\n  with " + PACKAGE + ".GlutenCheckedTest {")
        + ("" if suffix.startswith("\n") else "\n")
        + suffix
    )


def wrap_validation_calls(text):
    """Keep the added calls within Delta's line limit without reformatting test bodies."""
    lines = []
    for line in text.splitlines():
        if len(line) > 100:
            split = -1
            if "checkedContext(" in line:
                split = line.index("checkedContext(") + len("checkedContext(")
            elif ".glutenChecked" in line:
                split = line.index(".glutenChecked")
            if split >= 0:
                indent = re.match(r"[ \t]*", line)[0]
                lines.extend((line[:split], indent + "  " + line[split:]))
                continue
        lines.append(line)
    return "\n".join(lines) + "\n"


def install(delta, sources=SOURCES, support=SUPPORT):
    spark_tests = delta / "spark/src/test"
    roots = [
        spark_tests,
        delta / "spark-unified/src/test",
        delta / "spark-shared-tests/src/test",
    ]
    shared_count = 0
    for root in roots:
        for path in root.rglob("*.scala"):
            # Runtime sources are copied last and must retain the original Spark fixture.
            if PACKAGE.replace(".", "/") in path.as_posix():
                continue
            old = path.read_text(encoding="utf-8")
            new = shared_imports(old)
            if old != new:
                path.write_text(new, encoding="utf-8")
                shared_count += 1

    fixture_dir = spark_tests / "scala/org/apache/spark/sql/delta/test"
    primary = (fixture_dir / "DeltaSQLCommandTest.scala").read_text(encoding="utf-8")
    if SHARED_IMPORT not in primary:
        raise ValueError(
            "DeltaSQLCommandTest did not receive the checked shared fixture"
        )

    for name, anchor in (
        ("DeltaHiveTest", "  override protected def spark: SparkSession"),
        ("DeltaSessionQueryTest", "  override protected def sparkConf: SparkConf"),
    ):
        path = fixture_dir / (name + ".scala")
        text = add_hook(path.read_text(encoding="utf-8"), name)
        member = (
            "  override protected def glutenSession: "
            "Option[org.apache.spark.sql.SparkSession] = Some(spark)\n\n"
        )
        text = replace_once(text, anchor, member + anchor, name)
        path.write_text(text, encoding="utf-8")

    factory_names = [
        line.strip()
        for line in (support / "validation-factories.txt")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    for name in factory_names:
        path = spark_tests / "scala" / (name.replace(".", "/") + ".scala")
        text = add_hook(path.read_text(encoding="utf-8"), name.rsplit(".", 1)[1])
        text = add_import(text, CHECKED + "._")
        if name.endswith(".SparkThreadLocalForwardingSuite"):
            original = 'SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local"))'
            wrapped = "checkedContext(" + original + ")"
            wrapped_count = len(
                re.findall(r"checkedContext\(\s*" + re.escape(original) + r"\)", text)
            )
            if text.count(original) != 4 or wrapped_count not in (0, 4):
                raise ValueError(
                    "Expected four consistently wrapped thread-forwarding SparkContext factories"
                )
            if not wrapped_count:
                text = text.replace(original, wrapped)
        else:
            text = re.sub(
                r"\.getOrCreate\(\)(?!\s*\.glutenChecked)",
                ".getOrCreate().glutenChecked",
                text,
            )
            if not re.search(r"\.getOrCreate\(\)\s*\.glutenChecked", text):
                raise ValueError("No checked SparkSession factory found in " + name)
        path.write_text(wrap_validation_calls(text), encoding="utf-8")

    java_root = spark_tests / "java"
    java_fixture = java_root / "org/apache/spark/sql/delta/DeltaSQLCommandJavaTest.java"
    text = java_fixture.read_text(encoding="utf-8")
    if CHECKED + ".checkedSession(" not in text:
        text = replace_once(
            text,
            "return SparkSession.builder()",
            "return " + CHECKED + ".checkedSession(SparkSession.builder()",
            str(java_fixture),
        )
        text = replace_once(
            text, ".getOrCreate();", ".getOrCreate());", str(java_fixture)
        )
    java_fixture.write_text(text, encoding="utf-8")

    java_count = 0
    for path in java_root.rglob("*.java"):
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r"public class \w+\b[^{}]*\bimplements\b[^{}]*\bDeltaSQLCommandJavaTest\b[^{}]*\{",
            text,
        )
        if not match:
            continue
        java_count += 1
        if "new " + PACKAGE + ".GlutenValidationRule()" not in text:
            rule = (
                "\n  @org.junit.Rule\n"
                "  public final transient org.junit.rules.TestRule glutenValidation =\n"
                "      new " + PACKAGE + ".GlutenValidationRule();\n"
            )
            text = text[: match.end()] + rule + text[match.end() :]
        # Serializable Java suites can be captured by Spark closures; the driver-only rule cannot.
        text = replace_once(
            text,
            "public final org.junit.rules.TestRule glutenValidation",
            "public final transient org.junit.rules.TestRule glutenValidation",
            str(path),
        )
        path.write_text(text, encoding="utf-8")
    if not java_count:
        raise ValueError("No DeltaSQLCommandJavaTest implementations were guarded")

    destination = delta / "spark-unified/src/test/scala"
    copied = 0
    for kind in ("main", "test"):
        source_root = sources / kind / "scala"
        for path in source_root.rglob("*.scala"):
            target = destination / path.relative_to(source_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            # Delta requires its own copyright. Retain the foreign ASF header and all other checks.
            target.write_text(
                FOREIGN_HEADER_DIRECTIVE + path.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            copied += 1
    if copied != 3:
        raise ValueError(
            "Expected three Gluten validation Scala sources, found {}".format(copied)
        )
    grouping = (
        sources.with_name("src-delta-validation-sbt")
        / "main/scala/org/apache/gluten/ci/GlutenTestGrouping.scala"
    )
    (delta / "project").mkdir(exist_ok=True)
    shutil.copyfile(grouping, delta / "project/GlutenTestGrouping.scala")
    shutil.copyfile(
        support / "validation-factories.txt", delta / "gluten-validation-factories.txt"
    )
    print(
        "Installed Gluten validation: {} shared imports, {} Java suites, {} direct factories".format(
            shared_count, java_count, len(factory_names)
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("delta_dir", type=Path)
    args = parser.parse_args()
    try:
        install(args.delta_dir)
    except (OSError, ValueError) as error:
        print(
            "ERROR: cannot install Gluten runtime validation: " + str(error),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
