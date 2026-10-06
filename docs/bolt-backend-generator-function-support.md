---
layout: page
title: Bolt Backend Generator Function Support
nav_order: 23
---

# Spark Generator Functions Support Status in Bolt

- **Date**: 2026-01-08
- **Spark Version**: 3.5

## Support Matrix

| Spark Functions     | Bolt Plan/Operator              | Status (S/NS) | Notes/Restrictions                                                                                                                              |
| ------------------- | ------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `explode`           | `GeneratorNode` / `GeneratorExplode` | S             | Supports arrays (emits one column) and maps (emits key and value columns).                                                                      |
| `explode_outer`     | `GeneratorNode` / `GeneratorExplode` | S             | Handles `NULL` or empty inputs by producing `NULL` in the output columns, controlled by the `isOuter` flag.                                     |
| `posexplode`        | `GeneratorNode` / `GeneratorExplode` | S             | Generates an additional zero-based integer index column, controlled by the `withOrdinality` flag.                                              |
| `posexplode_outer`  | `GeneratorNode` / `GeneratorExplode` | S             | Combines `outer` semantics with positional indexing (`isOuter` + `withOrdinality` flags).                                                     |
| `inline`            | N/A                             | NS            | No dedicated generator or operator was found in the codebase. The function name exists in metadata lists but lacks an execution implementation. |
| `inline_outer`      | N/A                             | NS            | Same as `inline`; no specific implementation was found.                                                                                         |
| `stack`             | N/A         | NS  | The semantics of `ExpandNode` match Spark's `stack` function, which transforms one row into multiple.|
