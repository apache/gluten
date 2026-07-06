---
layout: page
title: Metrics Framework
nav_order: 15
parent: Developer Overview
---

# Metrics Framework

This document explains how Velox operator metrics are mapped back to Gluten
Spark SQL metrics. The mapping has three ordered steps:

1. Native code treefies the Velox plan into `orderedNodeIds`.
2. Scala parses the JSON payload into a flat `JList[OperatorMetrics]`.
3. Scala treefies the Spark plan into `MetricsUpdaterTree` and consumes the
   flattened native metrics in that order.

The JSON transport keeps the JNI boundary small and stable. C++ reports named
Velox stats in a deterministic order; Scala owns the mapping from those stats to
Gluten operator metrics.

## Mapping Overview

The metrics mapping joins three views of the same execution:

- Velox plan node ids and task stats from native execution.
- Substrait rel ids recorded in `operatorToRelsMap`.
- Spark physical operators, each with a `MetricsUpdater`.

During planning, Gluten assigns an operator id to each transform operator and
records the Substrait rel ids generated for that operator:

```text
operatorToRelsMap: Spark operator id -> Substrait rel ids
```

After execution, native code serializes Velox stats in `orderedNodeIds` order.
Scala parses that JSON into:

```text
Velox JSON node stats -> JList[OperatorMetrics]
```

Finally, `MetricsUtil.updateTransformerMetricsInternal` walks the
`MetricsUpdaterTree`, `operatorToRelsMap`, and native metric list together. The
current implementation consumes both Spark operator ids and native metric indexes
from the end:

```text
operatorIdx = relMap.size() - 1
metricsIdx  = nativeMetrics.size() - 1
```

Each Spark operator consumes the native metric suites that correspond to its
Substrait rel ids, merges or interprets them, and writes the final values into
Spark `SQLMetric`s.

## Step 1: Native Treefy to orderedNodeIds

`orderedNodeIds` is produced by `WholeStageResultIterator::getOrderedNodeIds`.
This is a native treefy process over the Velox plan. It converts the Velox plan
tree into a deterministic list of node ids that Scala can later flatten into
`OperatorMetrics`.

This step is required because Velox task stats are keyed by plan node id. A map
of stats does not preserve the traversal order needed by Gluten's metrics
updater tree. Native code must provide that order explicitly.

For ordinary Velox nodes, `getOrderedNodeIds` performs post-order traversal:

```text
visit all sources first
then append current node id
```

This gives Scala a list that matches Substrait rel order and can be consumed in
reverse by decrementing `metricsIdx`.

`getOrderedNodeIds` also encodes Velox-specific plan-shape adjustments that
Scala cannot reliably infer from task stats alone:

- For Project nodes, it visits the source first and then the Project node.
- If the Project source is a Filter node, Velox has mapped Filter over Project
  into a FilterProject operator. The Filter node has no independent stats, so
  native code records the Filter id in `omittedNodeIds`.
- For `LocalPartitionNode`, Velox may insert local exchange/partition nodes and
  optional projected children. Native code walks through projected children when
  present, otherwise through the source directly. When the node has two sources,
  it records the `LocalPartitionNode` id as the concrete Spark native union
  transformer.

The native treefy result becomes the ordering contract for the rest of the
framework:

```text
Velox plan tree
  -> getOrderedNodeIds
  -> orderedNodeIds + omittedNodeIds
```

Without `orderedNodeIds`, Scala would have to depend on the iteration order of
Velox's `planStats` map or reconstruct Velox-specific plan rewrites after the
fact. Either option would make metrics assignment fragile, especially for
operators that are fused, omitted, or expanded into multiple Velox nodes.

## Step 2: JSON to OperatorMetrics

After the Velox iterator finishes, C++ reads Velox task stats and serializes a
JSON payload with these fields:

- `orderedNodeIds`: Velox plan node ids in the native treefy order.
- `omittedNodeIds`: expected nodes that do not have Velox stats.
- `nodeStats`: per-node Velox operator stats.
- `loadLazyVectorTime`: Gluten lazy vector loading time.

Scala parses the payload in `MetricsUtil.parseNativeOperatorMetrics`:

1. Iterate through `orderedNodeIds`.
2. Look up each node id in `nodeStats`.
3. Convert every Velox operator stat for the node into `OperatorMetrics`.
4. If the node id is in `omittedNodeIds` and has no stat, insert an empty
   `OperatorMetrics` placeholder.
5. Attach `loadLazyVectorTime` to the last flattened native metric suite.
6. Validate that the parsed count matches `Metrics.numMetrics`.

This produces the flat `JList[OperatorMetrics]` that the Spark updater tree
will consume.

```text
orderedNodeIds: [n0, n1, n2]
nodeStats:
  n0 -> [stat0]
  n1 -> [stat1, stat2]
  n2 -> [stat3]

flattened nativeMetrics:
  [OperatorMetrics(stat0),
   OperatorMetrics(stat1),
   OperatorMetrics(stat2),
   OperatorMetrics(stat3)]
```

If a node is omitted, Scala still inserts a zero-value placeholder so native
metric indexes continue to line up with the updater traversal.

## Step 3: Spark Plan to MetricsUpdaterTree

`MetricsUtil.treeifyMetricsUpdaters(plan)` converts the Spark physical plan into
a tree of metrics updaters. This is the Spark-side treefy process. The resulting
tree describes which Spark operators should receive native metrics and in what
child order they should be traversed.

The important cases are:

- `HashJoinLikeExecTransformer`: creates a join updater node with children in
  `(buildPlan, streamedPlan)` order.
- `SortMergeJoinExecTransformer`: creates a join updater node with children in
  `(bufferedPlan, streamedPlan)` order.
- `TransformSupport` with `MetricsUpdater.None`: skips the current node and
  treeifies its child. This is used when a Spark node exists for planning shape
  but should not receive native metrics itself.
- Other `TransformSupport`: creates an updater node and treeifies
  `children.reverse`.
- Non-transform Spark nodes: become `MetricsUpdater.Terminate`, which stops
  native metric propagation for that branch.

The child reversal is intentional. Native metrics are later consumed from the
end of the flattened list, so the updater tree must mirror the order produced by
Substrait planning and native `orderedNodeIds`.

Conceptually:

```text
SparkPlan
  -> treeifyMetricsUpdaters
  -> MetricsUpdaterTree(updater, children)
```

The tree does not contain metric values. It only contains the updater topology
needed to replay native metrics onto Spark operators.

## Step 4: Consume and Map Suites

In the Scala mapping code, one `OperatorMetrics` object represents one native
metric suite. A suite usually corresponds to one Velox operator stat. Some Spark
operators consume one suite; others consume multiple suites because the Spark
operator expands to multiple Substrait/Velox operators.

The number of suites initially assigned to a Spark operator is:

```text
relMap(operatorIdx).size()
```

`updateTransformerMetricsInternal` performs the operator-level mapping. For each
updater node, it:

1. Reads the Substrait rel ids for the current `operatorIdx`.
2. Consumes one native `OperatorMetrics` suite per rel id.
3. Applies operator-specific handling.
4. Recurses into child updater nodes with decremented indexes.

For a normal unary operator, the consumed suites are merged and passed to:

```text
u.updateNativeMetrics(mergedOperatorMetrics)
```

The merge behavior is designed around Velox pipeline shape:

- Input-side counters are taken from the last consumed suite.
- Output-side and write counters are taken from the first consumed suite.
- CPU, wall time, spill, allocation, and most custom counters are accumulated.
- `loadLazyVectorTime` is attached to the final flattened suite and accumulated
  across consumed suites.
- Peak memory uses the maximum value.

This gives the Spark operator one coherent metric row even when it was
implemented by multiple native rels.

The alignment can be summarized as:

```text
native orderedNodeIds treefy
  -> flattened OperatorMetrics
  -> Spark MetricsUpdaterTree traversal
  -> Spark SQLMetric updates
```

## Operator-Specific Mapping

Some operators do not follow the simple "consume rel count, merge, update" rule.

### Joins

Join updaters consume the suites assigned by `relMap`, then consume one
additional suite for the build/probe side metrics. The updater also receives join
parameters from planning, so it can map Velox join-side values to the correct
Spark SQL metrics.

`HashJoinLikeExecTransformer` and `SortMergeJoinExecTransformer` also use custom
tree child ordering during Spark-side treefy, because build/buffered and
streamed sides must line up with the native traversal.

### Union

`UnionMetricsUpdater` consumes one extra suite and updates union-specific metrics
from the combined native values.

### Hash Aggregate

`HashAggregateMetricsUpdater` uses aggregation parameters recorded during
planning. The native suites still come from `relMap`, but the updater needs
those parameters to decide how aggregation metrics map to Spark metrics.

### Limit Over Sort

Velox may implement `Limit` over `Sort` as a TopN-style native operator. In that
case, the native metric suite belongs to the sort updater. The limit updater does
not update metrics and does not consume a suite, so the downstream indexes
remain aligned.

## End-to-End Example

For a simple transformed plan:

```text
Project
  Filter
    Scan
```

native treefy produces:

```text
orderedNodeIds:
  [scan node, filter node, project node]
```

Scala parses the JSON into:

```text
nativeMetrics:
  0 -> scan suite
  1 -> filter suite
  2 -> project suite
```

Spark-side treefy builds:

```text
ProjectUpdater
  FilterUpdater
    ScanUpdater
```

Suppose planning recorded:

```text
operatorToRelsMap:
  0 -> [scan rel]
  1 -> [filter rel]
  2 -> [project rel]
```

The updater starts from the end:

```text
operatorIdx = 2, metricsIdx = 2
```

It updates project first, then filter, then scan. Each step consumes the suite
for the current operator and decrements both indexes. More complex operators use
the same traversal but may consume more than one suite.

## Adding or Debugging a Mapping

When adding a metric or debugging a wrong value, follow the same path as runtime:

1. Check native `orderedNodeIds` and `omittedNodeIds` if the wrong Velox node is
   being flattened or a fused node is missing.
2. Check that `parseNativeOperatorMetrics` produces the expected number and
   order of `OperatorMetrics` suites.
3. Check the Spark metric key in `VeloxMetricsApi`.
4. Check the target `MetricsUpdater` to see how the `OperatorMetrics` suite is
   written to Spark SQL metrics.
5. Check whether the operator consumes a normal number of suites or has special
   handling in `updateTransformerMetricsInternal`.
6. Check Spark-side treefy child ordering if the wrong side of a join or
   multi-child operator is being updated.

The most useful invariant is:

```text
parsed native metric count == Metrics.numMetrics
```

If that holds but values are assigned to the wrong Spark operator, inspect the
native `orderedNodeIds`, the `MetricsUpdaterTree` shape, `operatorToRelsMap`,
and any operator-specific extra suite consumption.
