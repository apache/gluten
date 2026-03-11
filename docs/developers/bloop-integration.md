# Bloop Integration for Faster Scala Builds

Bloop is a build server for Scala that dramatically accelerates incremental compilation by maintaining a persistent JVM with warm compiler state. For Gluten development, this eliminates the ~52s Zinc analysis loading overhead that occurs with every Maven build.

## Benefits

- **Persistent incremental compilation**: Bloop keeps Zinc's incremental compiler state warm
- **Watch mode**: Automatic recompilation when files change (`bloop compile -w`)
- **Fast test iterations**: Skip Maven overhead for repeated test runs
- **IDE integration**: Metals/VS Code can use Bloop for builds

## Prerequisites

### Install Bloop CLI

Choose one of these installation methods:

```bash
# Using Coursier (recommended)
cs install bloop

# Using Homebrew (macOS)
brew install scalacenter/bloop/bloop

# Using SDKMAN
sdk install bloop

# Manual installation
# See https://scalacenter.github.io/bloop/setup
```

Verify installation:
```bash
bloop --version
```

## Setup

### Generate Bloop Configuration

Run the setup script with your desired Maven profiles:

```bash
# Velox backend with Spark 3.5
./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-velox

# Velox backend with Spark 4.0 (requires JDK 17)
./dev/bloop-setup.sh -Pjava-17,spark-4.0,scala-2.13,backends-velox,spark-ut

# ClickHouse backend
./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-clickhouse

# With optional modules
./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-velox,delta,iceberg
```

This generates `.bloop/` directory with JSON configuration files for each Maven module.

### Using the Maven Profile Directly

The `-Pbloop` profile automatically skips style checks during configuration generation. You can use it directly with Maven:

```bash
# These are equivalent:
./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-velox

# Manual invocation with profile
./build/mvn generate-sources bloop:bloopInstall -Pspark-3.5,scala-2.12,backends-velox,fast-build -DskipTests
```

The bloop profile sets these properties automatically:
- `spotless.check.skip=true`
- `scalastyle.skip=true`
- `checkstyle.skip=true`
- `maven.gitcommitid.skip=true`
- `remoteresources.skip=true`

**Note:** The setup script also injects JVM options (e.g., `--add-opens` flags) required for Spark tests on Java 17+. If you run `bloop:bloopInstall` manually without the script, tests may fail with `IllegalAccessError`. Use the setup script to ensure proper configuration.

### Common Profile Combinations

| Use Case | Profiles |
|----------|----------|
| Spark 3.5 + Velox | `-Pspark-3.5,scala-2.12,backends-velox` |
| Spark 4.0 + Velox | `-Pjava-17,spark-4.0,scala-2.13,backends-velox` |
| Spark 4.1 + Velox | `-Pjava-17,spark-4.1,scala-2.13,backends-velox` |
| With unit tests | Add `,spark-ut` to any profile |
| ClickHouse backend | Replace `backends-velox` with `backends-clickhouse` |
| With Delta Lake | Add `,delta` to any profile |
| With Iceberg | Add `,iceberg` to any profile |

## Usage

### Basic Commands

```bash
# List all projects
bloop projects

# Compile a project
bloop compile gluten-core

# Compile with watch mode (auto-recompile on changes)
bloop compile gluten-core -w

# Compile all projects
bloop compile --cascade gluten-core

# Run tests
bloop test gluten-core

# Run specific test suite
bloop test gluten-ut-spark35 -o GlutenSQLQuerySuite

# Run tests matching pattern
bloop test gluten-ut-spark35 -o '*Aggregate*'
```

### Running Tests

Use the convenience wrapper to match `run-scala-test.sh` interface:

```bash
# Run entire suite
./dev/bloop-test.sh -pl gluten-ut/spark35 -s GlutenSQLQuerySuite

# Run specific test method
./dev/bloop-test.sh -pl gluten-ut/spark35 -s GlutenSQLQuerySuite -t "test method name"

# Run with wildcard pattern
./dev/bloop-test.sh -pl gluten-ut/spark40 -s '*Aggregate*'
```

### Environment Variables

When running tests with bloop directly (not via `bloop-test.sh`), set these environment variables:

```bash
# Required for Spark 4.x tests - disables ANSI mode which is incompatible with some Gluten features
export SPARK_ANSI_SQL_MODE=false

# If bloop uses wrong JDK version, set JAVA_HOME before starting bloop server
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
bloop exit && bloop about  # Restart server with new JDK

# Then run tests
bloop test backends-velox -o '*VeloxHashJoinSuite*'
```

**Note:** The `bloop-test.sh` wrapper automatically sets `SPARK_ANSI_SQL_MODE=false`.

### Watch Mode for Rapid Development

Watch mode is ideal for iterative development:

```bash
# Terminal 1: Start watch mode for your module
bloop compile gluten-core -w

# Terminal 2: Edit files and see instant compilation feedback
# Errors appear immediately as you save files
```

## Comparison: Bloop vs Maven

| Aspect | Maven | Bloop |
|--------|-------|-------|
| First compilation | Baseline | Same (full build needed) |
| Incremental compilation | ~52s+ (Zinc reload) | <5s (warm JVM) |
| Watch mode | Not supported | Native support |
| Test execution | Full Maven lifecycle | Direct execution |
| IDE integration | Limited | Metals/VS Code native |
| Profile switching | Edit command | Re-run setup script |

### When to Use Each

**Use Bloop when:**
- Rapid iteration during development
- Running tests repeatedly
- Want instant feedback on changes
- Using Metals/VS Code

**Use Maven when:**
- CI/CD builds
- Full release builds
- First-time setup
- Switching between profile combinations
- Need Maven-specific plugins

## IDE Integration

### VS Code with Metals

1. Install Metals extension in VS Code
2. Generate bloop configuration: `./dev/bloop-setup.sh -P<profiles>`
3. Open the project folder in VS Code
4. Metals will detect `.bloop/` and use it for builds

### IntelliJ IDEA

IntelliJ uses its own incremental compiler by default. However, you can:
1. Use the terminal for bloop commands
2. Configure IntelliJ to use BSP (Build Server Protocol) with bloop

## Troubleshooting

### "Bloop project not found"

```
Error: Bloop project 'gluten-ut-spark35' not found
```

The project wasn't included in the generated configuration. Regenerate with the correct profiles:

```bash
# Make sure to include the spark-ut profile for test modules
./dev/bloop-setup.sh -Pspark-3.5,scala-2.12,backends-velox,spark-ut
```

### "Bloop CLI not found"

```
Error: Bloop CLI not found. Install with: cs install bloop
```

Install the bloop CLI:
```bash
# Using Coursier
cs install bloop

# Or check if it's in your PATH
which bloop
```

### Configuration Out of Sync

If compilation fails with unexpected errors, regenerate the configuration:

```bash
# Remove old config
rm -rf .bloop

# Regenerate
./dev/bloop-setup.sh -P<your-profiles>
```

### Bloop Server Issues

```bash
# Restart bloop server
bloop exit
bloop about  # This starts a new server

# Or kill all bloop processes
pkill -f bloop
```

### Profile Mismatch

Remember that bloop configuration is generated for a specific set of Maven profiles. If you need to switch profiles:

```bash
# Switching from Spark 3.5 to Spark 4.0
./dev/bloop-setup.sh -Pjava-17,spark-4.0,scala-2.13,backends-velox,spark-ut
```

## Advanced Usage

### Parallel Compilation

Bloop automatically uses parallel compilation. Control with:

```bash
# Limit parallelism
bloop compile gluten-core --parallelism 4
```

### Clean Build

```bash
# Clean specific project
bloop clean gluten-core

# Clean all projects
bloop clean
```

### Dependency Graph

```bash
# Show project dependencies
bloop projects --dot | dot -Tpng -o deps.png
```

## Notes

- **Configuration is not committed**: `.bloop/` is in `.gitignore` by design
- **Profile-specific**: Must regenerate when changing Maven profiles
- **Complements Maven**: Bloop accelerates development; Maven remains for CI/production builds
- **First run is slow**: Initial `bloopInstall` does full Maven resolution
