# Agent Guidelines for Apache Gluten

## Developer Documentation

- [New to Gluten](docs/developers/NewToGluten.md)
- [Contributing Guide](CONTRIBUTING.md)
- [Build Guide (Velox backend)](docs/get-started/Velox.md)
- [Build Guide (ClickHouse backend)](docs/get-started/ClickHouse.md)
- [C++ Coding Style](docs/developers/CppCodingStyle.md)

## Build Order (Important)

The Java/Scala build depends on native shared libraries produced by the C++ build.
Always build the native backend before running Maven. Modifying `gluten-core` without
rebuilding the native backend produces a build that compiles but fails at runtime.

```bash
# Velox backend — build native first, then Maven
./dev/builddeps-veloxbe.sh
mvn package -Pbackends-velox -DskipTests

# ClickHouse backend — build native first, then Maven
./dev/builddeps-clickhousebe.sh
mvn package -Pbackends-clickhouse -DskipTests
```

## Before Committing

Before committing any changes, you MUST run the following checks and fix any issues.

### Java/Scala

```bash
./dev/format-scala-code.sh
```

### C/C++ (Velox backend)

```bash
# requires clang-format-15
./dev/format-cpp-code.sh
```

### License Headers

```bash
# Requires the `regex` Python package: pip install regex
./dev/check.py header main --fix
```

Do not commit code that fails CI checks.

## PR Title Convention

Use `[GLUTEN-<issue ID>]` when a GitHub issue exists ("if necessary" per CONTRIBUTING.md —
omit the issue tag entirely for minor changes with no associated issue).

| Scope | Title format |
|---|---|
| Velox backend only | `[GLUTEN-<issue ID>][VL] description` |
| ClickHouse backend only | `[GLUTEN-<issue ID>][CH] description` |
| Common/core code | `[GLUTEN-<issue ID>][CORE] description` |
| Docs/minor | `[GLUTEN-<issue ID>][DOC]` or `[MINOR] description` |

Examples from CONTRIBUTING.md:
- `[GLUTEN-1234][VL] Fix null handling in velox aggregation`
- `[MINOR][DOC] Update configuration docs`

## Testing

Add at least one unit test (under `gluten-ut/`) for any fix or feature not covered by
existing Spark UTs.

```bash
# Java/Scala unit tests
mvn test -pl gluten-ut

# Velox backend (build with tests enabled)
./dev/builddeps-veloxbe.sh --build_tests=ON

# ClickHouse backend CI — re-trigger by posting on the PR page:
# Run Gluten Clickhouse CI
```

## AI Tooling Disclosure

The PR template requires answering: "Was this patch authored or co-authored using
generative AI tooling?"

- If yes: `Generated-by: <tool name and version>` (e.g. `Generated-by: Claude claude-sonnet-4-6`)
- If no: write `No`

See [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html).
