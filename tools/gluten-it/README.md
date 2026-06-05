# Gluten Integration Testing (gluten-it)

The project makes it easy to test Gluten build locally.

## Gluten

Gluten is a native Spark SQL implementation as a standard Spark plug-in.

https://github.com/apache/gluten

## Getting Started

### 1. Build Gluten

See official Gluten build guidance https://github.com/apache/gluten#build-from-source.

### 2. Build and run gluten-it

```sh
cd gluten/tools/gluten-it
mvn clean package -P{Spark-Version}
sbin/gluten-it.sh
```

Note: **Spark-Version** can only be **spark-3.2**, **spark-3.3**, **spark-3.4** or **spark-3.5**.

## Usage

### CMD args

```
Usage: gluten-it [-hV] [COMMAND]
Gluten integration test using various of benchmark's data and queries.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  data-gen-only    Generate data only.
  queries          Run queries.
  queries-compare  Run queries and do result comparison with baseline preset.
  spark-shell      Open a standard Spark shell.
  parameterized    Run queries with parameterized configurations
```

Also, use `[COMMAND] -h` to view help message for a specific subcommand. For example:

```sh
sbin/gluten-it.sh queries -h
```
