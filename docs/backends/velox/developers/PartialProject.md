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

The partial projection feature can also benefit from expressions that are not natively supported. For example, `substr(from_json(col_a))`. Since `from_json` is not fully supported, Gluten may use the JVM code path. Instead of projecting the whole expression, partial projection will attempt to project `from_json()` and perform a native projection of `substr()`.

In the case of blacklisted expressions defined in `spark.gluten.expression.blacklist`, this feature is also beneficial. 

## Limitations

This feature is in the preliminary stages of development and will be improved in future updates.