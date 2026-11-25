---
layout: page
title: PartialProject
nav_order: 15
parent: Developer Overview
---

# Partial Projection Support

In Gluten, there is still a gap in supporting all Spark expressions natively (e.g., some JSON functions or Java UDFs). In this case, Gluten will choose the JVM code path to run the expressions, which can introduce performance regressions.

Partial projections, which allow Gluten to minimal data copy between JVM and C++, were added to avoid these performance regressions. 


## Detailed Implementations

### Adding Partial Projection for UDF

For example, with the expression `hash(udf(col0)), col1, col2, col3, col4`, partial projection allows us to convert only `col0` to row or column to Arrow as input, and convert `udf(col0)` as an alias `partialProject1_`. Then, ProjectExecTransformer will handle `hash(partialProject1_), col1, col2, col3, col4, partialProject1_`. This feature saves the cost of converting the columnar format to row format and vice-versa.


## Adding Partial Projection for Unsupported Expressions

Partial projection can also run a built-in expression on the JVM while keeping the rest of the projection native.
For example, assume `json_input` is a Parquet table with columns `id` and `json_text` (a string containing JSON):

```sql
SET spark.gluten.sql.columnar.partial.project=true;
SET spark.gluten.expression.blacklist=from_json;

SELECT id, substring(from_json(json_text, 'name STRING').name, 1, 3) AS name_prefix
FROM json_input;
```

The blacklist makes `from_json` run on the JVM. The `.name` access passes a string to `substring`, not the parsed struct.
If the scan and the remaining expressions can run natively and partial projection passes validation,
`ColumnarPartialProject` evaluates `from_json` and passes its result to the native projection.
Use `EXPLAIN FORMATTED` to check which path the query takes.

See [JSON function fallback](../velox-backend-limitations.md#json-function-fallback) for cases where Velox declines to run `from_json` or `to_json` natively.

## Limitations

This feature is in the preliminary stages of development and will be improved in future updates.