# Agent Guidelines for Apache Gluten

Entry point for AI coding agents and new contributors. Detailed build, test
and contribution docs are linked below — this file only captures the rules
that are easy to get wrong.

## Developer Documentation

- [New to Gluten](docs/developers/NewToGluten.md)
- [Contributing Guide](CONTRIBUTING.md)
- [Build Guide (Velox backend)](docs/get-started/Velox.md)
- [Build Guide (ClickHouse backend)](docs/get-started/ClickHouse.md)
- [C++ Coding Style](docs/developers/CppCodingStyle.md)

## Build Order (Important)

The Java/Scala build depends on native shared libraries produced by the C++
build. Always build the native backend before running Maven. Modifying
`gluten-core` without rebuilding the native backend produces a build that
compiles but fails at runtime.

Always invoke Maven through the `./build/mvn` wrapper — it pins the Maven
version and JVM flags Gluten expects.

```bash
# Velox backend — one-shot native + bundled jar build.
# PROMPT_ALWAYS_RESPOND=y auto-confirms vcpkg / system-package prompts.
export PROMPT_ALWAYS_RESPOND=y
./dev/buildbundle-veloxbe.sh --enable_vcpkg=ON \
  --enable_s3=OFF --enable_gcs=OFF --enable_hdfs=OFF --enable_abfs=OFF

# ClickHouse backend — build native first, then Maven.
bash ./ep/build-clickhouse/src/build-clickhouse.sh
./build/mvn clean package -Pbackends-clickhouse -Pspark-3.3 -DskipTests
```

For incremental builds, cloud-FS flags, the Spark/Scala/JDK matrix, table-format
and shuffle profiles, see [docs/get-started/Velox.md](docs/get-started/Velox.md)
and [docs/get-started/ClickHouse.md](docs/get-started/ClickHouse.md).

## Before Committing

You MUST run the following checks and fix any issues before committing.

### Java/Scala

```bash
./dev/format-scala-code.sh
```

### C/C++ (Velox backend)

```bash
# Requires clang-format-15 specifically — other versions reformat differently and CI rejects the diff.
./dev/format-cpp-code.sh
```

### License Headers

```bash
# Requires the `regex` Python package: pip install regex
./dev/check.py header main --fix
```

Do not commit code that fails CI checks.

## Testing

Add at least one unit test (under `gluten-ut/`) for any fix or feature not
covered by existing Spark UTs.

```bash
# gluten-ut — must select backend, Spark and Scala profiles
./build/mvn test -pl gluten-ut -Pspark-ut -Pbackends-velox -Pspark-3.5 -Pscala-2.12

# Velox backend (build with native tests enabled)
./dev/builddeps-veloxbe.sh --build_tests=ON

# ClickHouse backend CI — re-trigger by posting on the PR page:
# Run Gluten Clickhouse CI
```

## PR Title Convention

Use `[GLUTEN-<issue ID>]` when a GitHub issue exists ("if necessary" per
CONTRIBUTING.md — omit the issue tag entirely for minor changes with no
associated issue).

| Scope | Title format |
|---|---|
| Velox backend only | `[GLUTEN-<issue ID>][VL] description` |
| ClickHouse backend only | `[GLUTEN-<issue ID>][CH] description` |
| Common/core code | `[GLUTEN-<issue ID>][CORE] description` |
| Docs/minor | `[GLUTEN-<issue ID>][DOC]` or `[MINOR] description` |

Examples from CONTRIBUTING.md:
- `[GLUTEN-1234][VL] Fix null handling in velox aggregation`
- `[MINOR][DOC] Update configuration docs`

## AI Tooling Disclosure

The PR template requires answering: "Was this patch authored or co-authored
using generative AI tooling?"

- If yes: `Generated-by: <tool name and version>` (e.g.
  `Generated-by: Claude claude-sonnet-4-6`)
- If no: write `No`

See [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html).

## Common Pitfalls

- **Bare `mvn` instead of `./build/mvn`** — the wrapper pins versions and JVM flags.
- **Editing `gluten-core` / `gluten-substrait` and skipping the native rebuild.**
  Maven still succeeds, but JNI/Substrait mismatches blow up at runtime. Re-run
  `./dev/builddeps-veloxbe.sh build_gluten_cpp` when in doubt.
- **Wrong `clang-format` version.** Anything but 15.x reformats differently and
  CI rejects the diff. Verify with `clang-format --version`.
- **Spark 4.x with JDK 8.** Always pair `-Pspark-4.0`/`-Pspark-4.1` with
  `-Pjava-17 -Pscala-2.13 -Dmaven.compiler.release=17`.
- **Per-module tests before `./build/mvn install`.** `gluten-ut` and
  `backends-velox` resolve sibling Gluten modules from the local Maven repo —
  run `./build/mvn install ... -DskipTests` once after a clean checkout.
