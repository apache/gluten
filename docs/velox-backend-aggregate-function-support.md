# Aggregate Functions Support Status

**Out of 76 aggregate functions in Spark 4.1, Gluten currently fully supports 52 functions and partially supports 1 function.**

The status applies to `spark.sql.ansi.enabled=false`. When ANSI mode is enabled, Gluten falls back to vanilla Spark
(see `spark.gluten.sql.ansiFallback.enabled`).

## Aggregate Functions

| Spark Functions         | Spark Expressions                  | Status   | Restrictions   |
|-------------------------|------------------------------------|----------|----------------|
| any                     | BoolOr                             | S        |                |
| any_value               | AnyValue                           | S        |                |
| approx_count_distinct   | HyperLogLogPlusPlus                | S        |                |
| approx_percentile       | ApproximatePercentile              |          |                |
| approx_top_k            | ApproxTopK                         |          |                |
| approx_top_k_accumulate | ApproxTopKAccumulate               |          |                |
| approx_top_k_combine    | ApproxTopKCombine                  |          |                |
| array_agg               | CollectList                        | S        |                |
| avg                     | Average                            | S        |                |
| bit_and                 | BitAndAgg                          | S        |                |
| bit_or                  | BitOrAgg                           | S        |                |
| bit_xor                 | BitXorAgg                          | S        |                |
| bitmap_and_agg          | BitmapAndAgg                       |          |                |
| bitmap_construct_agg    | BitmapConstructAgg                 | S        |                |
| bitmap_or_agg           | BitmapOrAgg                        |          |                |
| bool_and                | BoolAnd                            | S        |                |
| bool_or                 | BoolOr                             | S        |                |
| collect_list            | CollectList                        | S        |                |
| collect_set             | CollectSet                         | S        |                |
| corr                    | Corr                               | S        |                |
| count                   | Count                              | S        |                |
| count_if                | CountIf                            | S        |                |
| count_min_sketch        | CountMinSketchAggExpressionBuilder |          |                |
| covar_pop               | CovPopulation                      | S        |                |
| covar_samp              | CovSample                          | S        |                |
| every                   | BoolAnd                            | S        |                |
| first                   | First                              | S        |                |
| first_value             | First                              | S        |                |
| grouping                | Grouping                           | S        |                |
| grouping_id             | GroupingID                         | S        |                |
| histogram_numeric       | HistogramNumeric                   |          |                |
| hll_sketch_agg          | HllSketchAgg                       |          |                |
| hll_union_agg           | HllUnionAgg                        |          |                |
| kll_sketch_agg_bigint   | KllSketchAggBigint                 |          |                |
| kll_sketch_agg_double   | KllSketchAggDouble                 |          |                |
| kll_sketch_agg_float    | KllSketchAggFloat                  |          |                |
| kurtosis                | Kurtosis                           | S        |                |
| last                    | Last                               | S        |                |
| last_value              | Last                               | S        |                |
| listagg                 | ListAgg                            |          |                |
| max                     | Max                                | S        |                |
| max_by                  | MaxBy                              | S        |                |
| mean                    | Average                            | S        |                |
| median                  | Median                             |          |                |
| min                     | Min                                | S        |                |
| min_by                  | MinBy                              | S        |                |
| mode                    | ModeBuilder                        |          |                |
| percentile              | Percentile                         |          |                |
| percentile_approx       | ApproximatePercentile              |          |                |
| percentile_cont         | PercentileContBuilder              |          |                |
| percentile_disc         | PercentileDiscBuilder              |          |                |
| regr_avgx               | RegrAvgX                           | S        |                |
| regr_avgy               | RegrAvgY                           | S        |                |
| regr_count              | RegrCount                          | S        |                |
| regr_intercept          | RegrIntercept                      | S        |                |
| regr_r2                 | RegrR2                             | S        |                |
| regr_slope              | RegrSlope                          | S        |                |
| regr_sxx                | RegrSXX                            | S        |                |
| regr_sxy                | RegrSXY                            | S        |                |
| regr_syy                | RegrSYY                            | S        |                |
| skewness                | Skewness                           | S        |                |
| some                    | BoolOr                             | S        |                |
| std                     | StddevSamp                         | S        |                |
| stddev                  | StddevSamp                         | S        |                |
| stddev_pop              | StddevPop                          | S        |                |
| stddev_samp             | StddevSamp                         | S        |                |
| string_agg              | ListAgg                            |          |                |
| sum                     | Sum                                | S        |                |
| theta_intersection_agg  | ThetaIntersectionAgg               |          |                |
| theta_sketch_agg        | ThetaSketchAgg                     |          |                |
| theta_union_agg         | ThetaUnionAgg                      |          |                |
| try_avg                 | TryAverageExpressionBuilder        | S        |                |
| try_sum                 | TrySumExpressionBuilder            | PS       |                |
| var_pop                 | VariancePop                        | S        |                |
| var_samp                | VarianceSamp                       | S        |                |
| variance                | VarianceSamp                       | S        |                |

