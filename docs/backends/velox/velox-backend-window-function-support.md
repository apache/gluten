# Window Functions Support Status

**Out of 9 window functions in Spark 4.1, Gluten currently fully supports 9 functions.**

The status applies to `spark.sql.ansi.enabled=false`. When ANSI mode is enabled, Gluten falls back to vanilla Spark
(see `spark.gluten.sql.ansiFallback.enabled`).

## Window Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| cume_dist         | CumeDist            | S        |                |
| dense_rank        | DenseRank           | S        |                |
| lag               | Lag                 | S        |                |
| lead              | Lead                | S        |                |
| nth_value         | NthValue            | S        |                |
| ntile             | NTile               | S        |                |
| percent_rank      | PercentRank         | S        |                |
| rank              | Rank                | S        |                |
| row_number        | RowNumber           | S        |                |

