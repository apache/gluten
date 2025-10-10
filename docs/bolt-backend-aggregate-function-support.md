---
layout: page
title: Bolt Backend Aggregate Function Support
nav_order: 21
---

# Aggregate Functions Support Status in Bolt

**Out of 62 aggregate functions in Spark 3.5, Gluten currently fully supports 54 functions and partially supports 1 function for Bolt backend.**

## Aggregate Functions

| Spark Functions       | Spark Expressions                  | Status   | Restrictions   |
|-----------------------|------------------------------------|----------|----------------|
| any                   | BoolOr                             | S        |                |
| any_value             | AnyValue                           | S        |                |
| approx_count_distinct | HyperLogLogPlusPlus                | S        | Only supported when the data type of the child expression is one of Boolean, Byte, Short, Int, Long, Float, Double, String, Char, Decimal, Date, Timestamp, or Binary. |
| approx_percentile     | ApproximatePercentile              | S        |                |
| array_agg             | CollectList                        | S        |                |
| avg                   | Average                            | S        |                |
| bit_and               | BitAndAgg                          | S        |                |
| bit_or                | BitOrAgg                           | S        |                |
| bit_xor               | BitXorAgg                          | S        |                |
| bitmap_construct_agg  | BitmapConstructAgg                 |          |                |
| bitmap_or_agg         | BitmapOrAgg                        |          |                |
| bool_and              | BoolAnd                            | S        |                |
| bool_or               | BoolOr                             | S        |                |
| collect_list          | CollectList                        | S        |                |
| collect_set           | CollectSet                         | S        |                |
| corr                  | Corr                               | S        |                |
| count                 | Count                              | S        |                |
| count_if              | CountIf                            | S        |                |
| count_min_sketch      | CountMinSketchAggExpressionBuilder |          |                |
| covar_pop             | CovPopulation                      | S        |                |
| covar_samp            | CovSample                          | S        |                |
| every                 | BoolAnd                            | S        |                |
| first                 | First                              | S        |                |
| first_value           | First                              | S        |                |
| grouping              | Grouping                           | S        |                |
| grouping_id           | GroupingID                         | S        |                |
| histogram_numeric     | HistogramNumeric                   |          |                |
| hll_sketch_agg        | HllSketchAgg                       |          |                |
| hll_union_agg         | HllUnionAgg                        |          |                |
| kurtosis              | Kurtosis                           | S        |                |
| last                  | Last                               | S        |                |
| last_value            | Last                               | S        |                |
| max                   | Max                                | S        |                |
| max_by                | MaxBy                              | S        |                |
| mean                  | Average                            | S        |                |
| median                | Median                             | S        |                |
| min                   | Min                                | S        |                |
| min_by                | MinBy                              | S        |                |
| mode                  | Mode                               |          |                |
| percentile            | Percentile                         | S        |                |
| percentile_approx     | ApproximatePercentile              | S        |                |
| regr_avgx             | RegrAvgX                           | S        |                |
| regr_avgy             | RegrAvgY                           | S        |                |
| regr_count            | RegrCount                          | S        |                |
| regr_intercept        | RegrIntercept                      | S        |                |
| regr_r2               | RegrR2                             | S        |                |
| regr_slope            | RegrSlope                          | S        |                |
| regr_sxx              | RegrSXX                            | S        |                |
| regr_sxy              | RegrSXY                            | S        |                |
| regr_syy              | RegrSYY                            | S        |                |
| skewness              | Skewness                           | S        |                |
| some                  | BoolOr                             | S        |                |
| std                   | StddevSamp                         | S        |                |
| stddev                | StddevSamp                         | S        |                |
| stddev_pop            | StddevPop                          | S        |                |
| stddev_samp           | StddevSamp                         | S        |                |
| sum                   | Sum                                | S        |                |
| try_avg               | TryAverageExpressionBuilder        | S        |                |
| try_sum               | TrySumExpressionBuilder            | PS       | Partially supported. Behavior may differ from vanilla Spark for some cases. |
| var_pop               | VariancePop                        | S        |                |
| var_samp              | VarianceSamp                       | S        |                |
| variance              | VarianceSamp                       | S        |                |
