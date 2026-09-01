---
layout: page
title: Substrait Modifications
nav_order: 9
parent: Developer Overview
---
# Substrait Modifications in Gluten

Substrait is a project aiming to create a well-defined, cross-language specification for data compute operations.
Since it is still under active development, there are some lacking representations for Gluten needed computing
operations. At the same time, some existing representations need to be modified a bit to satisfy the needs of computing.


In Gluten, the base version of Substrait is `v0.23.0`, with some messages since rebased onto later upstream
versions (noted per entry below). This page records all the Gluten changes to Substrait proto
files for reference. It is preferred to upstream these changes to Substrait, but for those cannot be upstreamed,
alternatives like `AdvancedExtension` could be considered.

**Numbering convention for local additions.** Gluten-local fields are numbered from `1000` up, outside the range
upstream allocates, so that a future upstream field addition cannot collide with them. This convention exists
because the older practice of grafting local fields onto the next free low number caused collisions: Gluten's
`bucket_spec` was originally grafted at `WriteRel` field 7, which upstream `v0.98.0` later assigned to `common`.

## Modifications to algebra.proto

* Added `JsonReadOptions` and `TextReadOptions` in `FileOrFiles`([#1584](https://github.com/apache/gluten/pull/1584)).
* Changed join type `JOIN_TYPE_SEMI` to `JOIN_TYPE_LEFT_SEMI` and `JOIN_TYPE_RIGHT_SEMI`([#408](https://github.com/apache/gluten/pull/408)).
* Added `WindowRel`, added `column_name` and `window_type` in `WindowFunction`,
changed `Unbounded` in `WindowFunction` into `Unbounded_Preceding` and `Unbounded_Following`, and added WindowType([#485](https://github.com/apache/gluten/pull/485)).
* Added `output_schema` in RelRoot([#1901](https://github.com/apache/gluten/pull/1901)).
* Added `ExpandRel`([#1361](https://github.com/apache/gluten/pull/1361)).
* Added `GenerateRel`([#574](https://github.com/apache/gluten/pull/574)).
* Added `PartitionColumn` in `LocalFiles`([#2405](https://github.com/apache/gluten/pull/2405)).
* Added `WriteRel` ([#3690](https://github.com/apache/gluten/pull/3690)).
* Added `TopNRel` ([#5409](https://github.com/apache/gluten/pull/5409)).
* Added `ref` field in window bound `Preceding` and `Following` ([#5626](https://github.com/apache/gluten/pull/5626)).
* Added `BucketSpec` field in `WriteRel`([#8386](https://github.com/apache/gluten/pull/8386))
* Added `StreamKafka` in `ReadRel`([#8321](https://github.com/apache/gluten/pull/8321))
* Rebased the `WriteRel` body onto upstream `v0.98.0`: field 7 is now `common`, with `create_mode` (8) and
`advanced_extension` (9) added, and the `OutputMode` value `OUTPUT_MODE_MODIFIED_TUPLES` renamed to
`OUTPUT_MODE_MODIFIED_RECORDS`. Gluten's `BucketSpec` field moved off field 7 to 1000. The enclosing
`Rel.write` oneof tag is left at 18 for now; reconciling the whole `Rel` oneof to upstream's numbers is a
separate change. Note that Gluten attaches its writer configuration to `named_table.advanced_extension`, not
to the new top-level `WriteRel.advanced_extension`, which no Gluten code reads. `WriteRel.common` uses
Gluten's pre-0.98 `RelCommon` copy (missing `rel_anchor`, `Hint.alias`, `Hint.output_names` and the
saved/loaded computation messages), so it cannot carry a full 0.98 `common` payload.

## Modifications to type.proto

* Added `Nothing` in `Type`([#791](https://github.com/apache/gluten/pull/791)).
* Added `names` in `Struct`([#1878](https://github.com/apache/gluten/pull/1878)).
* Added `PartitionColumns` in `NamedStruct`([#320](https://github.com/apache/gluten/pull/320)).
* Remove `PartitionColumns` and add `column_types` in `NamedStruct`([#2405](https://github.com/apache/gluten/pull/2405)).
