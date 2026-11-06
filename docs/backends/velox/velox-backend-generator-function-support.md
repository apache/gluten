# Generator Functions Support Status

**Out of 9 generator functions in Spark 4.1, Gluten currently fully supports 7 functions.**

The status applies to `spark.sql.ansi.enabled=false`. When ANSI mode is enabled, Gluten falls back to vanilla Spark
(see `spark.gluten.sql.ansiFallback.enabled`).

## Generator Functions

| Spark Functions   | Spark Expressions           | Status   | Restrictions   |
|-------------------|-----------------------------|----------|----------------|
| collations        |                             |          |                |
| explode           | ExplodeExpressionBuilder    | S        |                |
| explode_outer     | ExplodeExpressionBuilder    | S        |                |
| inline            | InlineExpressionBuilder     | S        |                |
| inline_outer      | InlineExpressionBuilder     | S        |                |
| posexplode        | PosExplodeExpressionBuilder | S        |                |
| posexplode_outer  | PosExplodeExpressionBuilder | S        |                |
| sql_keywords      |                             |          |                |
| stack             | Stack                       | S        |                |

