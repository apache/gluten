---
layout: page
title: Arrow Columnar Conversion
nav_order: 21
parent: /developer-overview/
---

# Arrow Columnar Conversion in Gluten

This document describes how Gluten converts data between rows, Spark columnar batches, Java Arrow
batches, native Arrow batches and Velox batches today, and how to add a new operator that consumes
or produces Arrow batches, using the Lance scan offload
([#12958](https://github.com/apache/gluten/pull/12958)) as the example.

## 1. Conventions

Every physical plan node in a Gluten query has a *convention*: the row type and the batch type it
outputs. Gluten never asks a node to convert its output itself. Instead, each type registers the
transitions to and from other types, and the planner inserts the transition operators between a
child and a parent whose conventions don't match.

![Gluten batch type transition graph](../../../image/GlutenConversionImages.png)


| Concept | Code | Meaning |
|---|---|---|
| `Convention.RowType` | `gluten-core/.../transition/Convention.scala` | A row format. `VanillaRowType` is Spark's `InternalRow`; `None` means the node outputs no rows |
| `Convention.BatchType` | same | A columnar batch format. `VanillaBatchType` is Spark's `ColumnarBatch`; `None` means no batches |
| `ConventionReq` | `ConventionReq.scala` | What a parent requires from each child: `Any` or `Is(type)`, for rows and for batches |
| `Transition` | `Transition.scala`, `TransitionGraph.scala` | An edge of the graph: a function `SparkPlan => SparkPlan` that wraps the child with a converting operator, or `Transition.empty` when the data is already compatible |
| `ConventionFunc` | `ConventionFunc.scala` | Derives the convention of any plan, including vanilla Spark plans and plans through `QueryStageExec` / `ReusedExchangeExec` |

A type registers its transitions in `registerTransitions()` with `fromRow`, `toRow`, `fromBatch` and
`toBatch`. All edges form one graph, and the planner uses the shortest path between two types
(`FloydWarshallGraph`), so a conversion over several steps is assembled automatically.

How `ConventionFunc` derives a convention:

- A `GlutenPlan` declares it: `batchType()` and `rowType()`. `supportsColumnar` and
  `supportsRowBased` are derived from them and are `final`.
- A vanilla Spark plan gets `VanillaBatchType` if `supportsColumnar`, and `VanillaRowType` if it
  supports rows.
- Components can override both through `Component.convFuncOverride()`.

How the planner requires a convention from children (`conventionReqOf`):

- A `GlutenPlan` declares `requiredChildConvention()`. By default, children must have the parent's
  own convention; a node that supports both rows and batches requires only its batch type.
- `ColumnarToRowExec` requires `VanillaBatchType`; `RowToColumnarExec` and `UnionExec` require
  `VanillaRowType`.

`InsertTransitions` removes existing transitions, then for each node compares
`conventionReqOf(node)` with `conventionOf(child)` and applies the transition path found in the
graph.

## 2. Types and how batches look

### Types registered with the Velox backend

| Type | Defined in | Physical form | Notes |
|---|---|---|---|
| `VanillaRowType` | `gluten-core` | Spark `InternalRow` | |
| `VanillaBatchType` | `gluten-core` | Spark `ColumnarBatch` of any `ColumnVector` | Transitions are Spark's `RowToColumnarExec` / `ColumnarToRowExec` |
| `ArrowJavaBatchType` | `gluten-arrow` `ArrowBatchTypes` | *Heavy* batch: every column is a `RefCountedArrowColumnVector`, Spark's `ArrowColumnVector` with a reference count | Readable from the JVM through Spark's `ArrowColumnVector` getters |
| `ArrowNativeBatchType` | `gluten-arrow` `ArrowBatchTypes` | *Light* batch: column 0 is an `IndicatorVector` holding the handle of a native `ColumnarBatch` (e.g. `ArrowCStructColumnarBatch`), the other columns are `PlaceholderVector`s | Not readable from the JVM |
| `VeloxBatchType` | `backends-velox` `VeloxBatchType` | Light batch whose native object is a `VeloxColumnarBatch` (a Velox `RowVector`) | |
| `VeloxCarrierRowType` | `backends-velox` `VeloxCarrierRowType` | Rows that carry a Velox batch | Used for row-based outputs that keep Velox data, e.g. for writes |

`ColumnarBatches` (`gluten-arrow/.../columnarbatch/ColumnarBatches.java`) tells heavy and light
batches apart, and holds the reference counts of both: `IndicatorVector` for light batches and
`RefCountedArrowColumnVector` for heavy ones. `ArrowColumnVectors`
(`gluten-arrow/.../vectorized/ArrowColumnVectors.scala`) builds heavy batches:

| Method | Use |
|---|---|
| `allocateRoot(schema)` | Allocates an empty `VectorSchemaRoot` from Gluten's task allocator, to fill directly |
| `toBatch(root)` / `wrap(vectors)` | Wraps Arrow vectors into `RefCountedArrowColumnVector`s, which take ownership of them |
| `fromRows(schema, rows)` | Writes `InternalRow`s into a new batch with Spark's `ArrowWriter` |
| `adopt(vector, allocator)` | Takes ownership of a vector: moved if it shares the allocator's root, copied otherwise |

### Transition graph

![Gluten batch type transition graph](../../../image/GlutenBatchTypeTransition.png)

- **`ArrowJavaBatchType` → `VanillaBatchType` is empty** because `RefCountedArrowColumnVector` is a
  Spark `ArrowColumnVector`. A vanilla consumer such as `ColumnarToRowExec` reads Gluten's Java Arrow
  batches directly.
- **`VeloxBatchType` → `ArrowNativeBatchType` is empty** because a `VeloxColumnarBatch` is a native
  `ColumnarBatch` that can export itself as Arrow C data, which is all that `ArrowNativeBatchType`
  promises.

Composite paths that the planner builds from these edges:

| From → To | Operators inserted |
|---|---|
| Velox → Java Arrow | `LoadArrowDataExec` (Velox → Arrow native is empty) |
| Java Arrow → Velox | `OffloadArrowDataExec` → `ArrowColumnarToVeloxColumnarExec` |
| Java Arrow → rows | `ColumnarToRowExec` (Java Arrow → vanilla batch is empty) |
| Rows → Java Arrow | `RowToVeloxColumnarExec` → `LoadArrowDataExec` |
| Velox → rows | `VeloxColumnarToRowExec` |

## 3. What each transition does

| Transition | Implementation | What happens to the data |
|---|---|---|
| `LoadArrowDataExec` (native → Java Arrow) | `ColumnarBatches.load` | The native batch exports Arrow C structs (`ColumnarBatchJniWrapper.exportToArrow`). For a Velox batch, `VeloxColumnarBatch::exportArrowArray` first flattens dictionary and constant vectors (`ensureFlattened`), then calls `velox::exportToArrow`. Java imports the arrays and wraps them as `RefCountedArrowColumnVector`s (`ArrowAbiUtil.importToSparkColumnarBatch` → `ArrowColumnVectors.toBatch`) |
| `OffloadArrowDataExec` (Java Arrow → native) | `ColumnarBatches.offload` | Java exports the vectors as Arrow C structs (`ArrowAbiUtil.exportFromSparkColumnarBatch`), and native wraps them as an `ArrowCStructColumnarBatch` (`createWithArrowArray`). No copy. A batch of plain Spark `ArrowColumnVector`s (not reference-counted, e.g. from a Spark Arrow operator) is first adopted into a new Gluten batch (`ArrowColumnVectors.adopt`), and the input is left to its producer |
| `ArrowColumnarToVeloxColumnarExec` (native Arrow → Velox) | `VeloxColumnarBatches.toVeloxBatch` → `VeloxColumnarBatch::from` | `velox::importFromArrowAsOwner` builds a Velox `RowVector` that owns the Arrow buffers |
| `RowToVeloxColumnarExec` / `VeloxColumnarToRowExec` | `backends-velox` | Converts between rows and Velox vectors (always copies) |
| `ColumnarToRowExec` over Java Arrow | Spark | Reads rows through Spark's `ArrowColumnVector` getters |

### Copies on the Arrow path

| Step | Fixed-width columns | Strings | Dictionary / constant vectors |
|---|---|---|---|
| Velox → Java Arrow (`load`) | No copy | Copied: Velox's `StringView` layout is exported as an Arrow string array, since Arrow string views aren't enabled | Flattened (copied) |
| Java Arrow → native Arrow (`offload`) | No copy | No copy | Not used |
| Plain Spark `ArrowColumnVector`s → Gluten batch (`adopt`, before `offload`) | Moved if the vectors share the root of Gluten's task allocator, copied otherwise | Same | Not used |
| Native Arrow → Velox | No copy | A new `StringView` buffer is built (strings of 12 bytes or less are inlined) | Not used |

### Batch lifecycle

- `load`, `offload` and `toVeloxBatch` convert **in place**: the new vectors are moved into the
  input `ColumnarBatch` object (`SparkColumnarBatchUtil.transferVectors`), and the output keeps the
  input's reference count. Code that holds the input object sees the converted batch.
- A batch is released with `ColumnarBatches.release` or `ColumnarBatch.close()`, which decrements
  the reference count of its vectors. An operator that needs a batch after its child has moved on
  must retain it (`ColumnarBatches.retain`) and release it later, as `ColumnarArrowEvalPythonExec`
  does.
- `ColumnarToColumnarExec` closes the batches it emits on the next call only when
  `needRecyclePayload` is true (e.g. `VeloxResizeBatchesExec`). The Arrow transitions keep the
  default, `false`.
- Java Arrow memory comes from `ArrowBufferAllocators.contextInstance()`, Gluten's per-task
  allocator, whose listener reports to Spark's memory manager.

## 4. Example: a Velox column through an Arrow UDF

This follows one fixed-width column, a `BIGINT` column `x`, through
`Velox op → LoadArrowDataExec → ColumnarArrowEvalPythonExec → OffloadArrowDataExec →
ArrowColumnarToVeloxColumnarExec → Velox op`, for a query such as `SELECT x, f(x)` with a pandas or
Arrow UDF `f`. .

![A Velox column through an Arrow UDF](../../../image/VeloxColumnArrowUdfLifecycle.png)

Three counts are involved, and none of them crosses JNI:

- **Velox buffer references** (`BufferPtr`), held by Velox vectors and by an exported `ArrowArray`.
- **Arrow Java `ArrowBuf` references**. An imported array is released through its C `release`
  callback once its last `ArrowBuf` is released.
- **Gluten's `refCnt`** on `RefCountedArrowColumnVector`. It only decides when Gluten closes the Java
  vector.

The Arrow C Data interface has no reference count. Ownership of an `ArrowArray` moves once, and the
holder calls `release` exactly once.

| Step | Velox buffer refs | `x.refCnt` | What happens |
|---|---|---|---|
| 1. Export | 2 | – | `VeloxColumnarBatch::exportArrowArray` exports `ArrowArray` A, which keeps a reference to the buffer. No copy for a flat fixed-width vector |
| 2. Import | 2 | 1 | `Data.importIntoVectorSchemaRoot` wraps the Velox memory as `ArrowBuf`s, and `ArrowColumnVectors.toBatch` creates `x` |
| 3. Close the Velox batch | 1 | 1 | `load` closes the input batch: `ObjectStore::release` frees the `RowVector`. A keeps the buffer alive |
| 4. Retain for the UDF | 1 | 2 | `ColumnarArrowEvalPythonExec` retains the input batch until the result comes back |
| 5. Send to Python | 1 | 2 | `ArrowStreamWriter` serializes the UDF columns into the socket. The Python worker reads its own copy; when pyarrow releases it, only Python's memory is freed |
| 6. Join the result | 1 | 1 | The output batch is `[x, r]`. `adjustRefCnt` sets every column to 1, because `offload` requires one count for the whole batch |
| 7. Export to native | 1 | 0 | `offload` exports `ArrowArray` B, which retains the `ArrowBuf`s, then closes the input batch. `x` is closed, and B keeps the memory alive |
| 8. Import into Velox | 1 | – | `importFromArrowAsOwner(B)` builds a Velox vector over the same memory. No copy |
| 9. Release | 0 | – | When the downstream Velox operator frees the vector, B's `release` (Arrow Java's export callback) drops the last `ArrowBuf`, which calls A's `release` (Velox's export callback). The buffer goes back to Velox's memory pool |

So the column's memory is freed by the last Velox operator that uses it, through two `release`
callbacks: Velox → Arrow Java → Velox. If the UDF output goes to a vanilla Spark operator instead,
step 7 doesn't happen, and the memory is freed when `ColumnarArrowEvalPythonExec` closes its output
batch on the next call (`recyclePayload`).

<details>
<summary>PlantUML source: Velox column through an Arrow UDF</summary>

```plantuml
@startuml VeloxColumnArrowUdfLifecycle
skinparam shadowing false
skinparam sequenceMessageAlign left
skinparam responseMessageBelowArrow true
hide footbox

title A Velox BIGINT column through an Arrow UDF and back (Gluten main)

box "Native (C++)" #EEF7EE
participant "Velox buffer\n(values of x)" as Buf
participant "VeloxColumnarBatch\n(upstream op)" as VB
participant "Velox\n(downstream op)" as VD
end box
box "JVM" #E8F0FB
participant "LoadArrowDataExec\nColumnarBatches.load" as Load
participant "RefCountedArrowColumnVector x\n(Arrow BigIntVector)" as X
participant "ColumnarArrowEvalPythonExec" as Py
participant "OffloadArrowDataExec\n+ ArrowColumnarToVelox" as Off
end box
participant "Python worker\n(pyarrow UDF)" as W

== 1. Velox -> Arrow C -> Java (no copy) ==
Load -> VB : JNI exportToArrow(handle)
VB -> VB : ensureFlattened()\nvelox::exportToArrow()
VB -> Buf : ArrowArray A keeps a buffer ref\n<b>Velox refs: 2</b> (vector + A)
VB --> Load : ArrowArrayMove(A)
Load -> X : Data.importIntoVectorSchemaRoot(A)\nArrowBuf wraps Velox memory\nArrowColumnVectors.toBatch()  <b>x.refCnt = 1</b>
Load -> VB : input.close() -> ObjectStore::release\nRowVector freed
VB -> Buf : <b>Velox refs: 1</b> (only A)

== 2. To Python and back (IPC copy) ==
Py -> X : ColumnarBatches.retain  <b>x.refCnt = 2</b>\ncache input batch
Py -> W : ArrowStreamWriter.writeBatch\n(serialize x into the socket)
W -> W : pyarrow reads its own copy,\nruns the UDF, frees its copy\n(no effect on JVM memory)
W --> Py : result batch r (IPC)
Py -> X : join [x, r]\nadjustRefCnt(x, 1)  <b>x.refCnt = 1</b>

== 3. Java -> Arrow C -> Velox (no copy) ==
Off -> X : Data.exportVectorSchemaRoot -> ArrowArray B\nB retains the ArrowBufs
Off -> X : offload closes the input batch\n<b>x.refCnt = 0</b> -> vector.close()\n(ArrowBufs still held by B)
Off -> VD : createWithArrowArray(B)\nimportFromArrowAsOwner(B)
note right of VD : Velox reads the same memory

== 4. Release chain ==
VD -> VD : downstream op done with the vector
VD -> X : B.release()  (Arrow Java export callback)\nlast ArrowBuf released
X -> Buf : A.release()  (Velox export callback)\n<b>Velox refs: 0</b>, buffer returned to the MemoryPool
@enduml
```

</details>

### Buffer release

Neither `RefCountedArrowColumnVector.close()` nor `B.release()` checks where a buffer came from.
Both drop one reference on each `ArrowBuf`, and when the last one goes, the buffer's
`AllocationManager`, chosen when the buffer was created, does the rest:

- **Allocated in Java** (`allocator.buffer(...)`, e.g. by `ColumnarRangeExec` or the Python result
  reader): the memory is freed in Java.
- **Imported through C Data** (`Data.importIntoVectorSchemaRoot`): the buffer has a
  `ForeignAllocationManager`, whose `release0()` calls `ReferenceCountedArrowArray.release()`. When
  the last buffer of the imported array is released, it calls the array's C `release` callback,
  here Velox's. It's the `release` member function of Arrow C_data interface:

```c++
struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

```

In both cases the allocator then calls `listener.onRelease(size)`, and Gluten's
`ManagedAllocationListener` returns the memory to Spark.

![How an ArrowBuf is released](../../../image/ArrowBufReleaseDispatch.png)

<details>
<summary>PlantUML source: release</summary>

```plantuml
@startuml ArrowBufReleaseDispatch
skinparam shadowing false
skinparam sequenceMessageAlign left
skinparam responseMessageBelowArrow true
hide footbox

title How an ArrowBuf is released (Arrow Java 15)

participant "Caller" as C
participant "ArrowBuf /\nBufferLedger" as L
participant "AllocationManager" as M
participant "Gluten allocator\n+ ManagedAllocationListener" as A
participant "ReferenceCountedArrowArray\n(imported ArrowArray A)" as R
participant "Native producer\n(Velox export)" as N

note over C
  Two callers, same path:
  - RefCountedArrowColumnVector.close(), when refCnt reaches 0
    -> ArrowColumnVector.close() -> FieldVector.close()
  - B.release() -> ExportedArrayPrivateData.close()
    (drops the reference the export retained)
end note

C -> L : close() -> release()
L -> L : bufRefCnt - 1
alt bufRefCnt > 0
  L --> C : still in use, nothing else happens
else bufRefCnt == 0
  L -> M : release(ledger)
  M -> A : releaseBytes(size)
  alt allocated in Java (allocator.buffer)
    M -> M : release0(): free the memory
  else imported through C Data (ForeignAllocationManager)
    M -> R : release0() -> ForeignAllocation.release0()\n-> ReferenceCountedArrowArray.release()
    R -> R : count - 1 (one per imported buffer)
    opt count == 0 (last buffer of the array)
      R -> N : ArrowArray.release() (C callback)
      N -> N : drop BufferPtr\nVelox frees the buffer
    end
  end
  M -> A : listener.onRelease(size)\nrepay Spark memory
end

== When the buffer was created ==
note over A
  allocator.buffer(size): onPreAllocation borrows from Spark, then allocates.
  wrapForeignAllocation(size): onPreAllocation also borrows from Spark,
  but the memory is Velox's, already counted by Velox's pool.
end note
@enduml
```

</details>

## 5. Arrow-based operators today

| Operator | Batch type in → out | How it's planned |
|---|---|---|
| `ColumnarArrowEvalPythonExec` (Arrow / pandas UDFs) | Java Arrow → Java Arrow | Gluten's own operator and Python runner, from `OffloadSingleNodeRules` |
| `ColumnarPartialProjectExec`, `ColumnarPartialGenerateExec` | Velox → Velox | Load to Java Arrow inside the operator, evaluate the Spark expressions row by row (`MutableProjection` / the generator), and write the results with Spark's `ArrowWriter` (`ArrowColumnVectors.fromRows`) |
| `ColumnarRangeExec` | – → Java Arrow | Fills a `BigIntVector` from `ArrowColumnVectors.allocateRoot` and wraps it with `toBatch` |
| `VeloxRDDScanTransformer`, `VeloxLocalTableScanTransformer` | – → Velox | Convert rows to Velox inside the operator, through the same JNI path as `RowToVeloxColumnarExec` (`toColumnarBatchIterator`) |
| `LanceScanTransformer` (`-Plance`) | – → Java Arrow | Imports an Arrow C stream from lance-spark and wraps the vectors with `ArrowColumnVectors.wrap` |

Any other Spark operator that wants Arrow, such as `MapInArrowExec` or `FlatMapGroupsInArrowExec`,
is a vanilla row-based plan to Gluten, so Gluten plans `VeloxColumnarToRow → Spark operator →
RowToVeloxColumnar` around it.

## 6. Adding an Arrow-based columnar operator

The Lance scan offload ([#12958](https://github.com/apache/gluten/pull/12958)) is a complete example: a leaf operator that produces Java Arrow batches from an external Arrow C stream,
and leaves every conversion to the planner. Its files:

| File | Role |
|---|---|
| `backends-velox/src-lance/main/scala/.../execution/LanceScanTransformer.scala` | The operator and its RDD |
| `backends-velox/src-lance/main/scala/.../extension/OffloadLanceScan.scala` | The rule that replaces `BatchScanExec` |
| `backends-velox/src-lance/main/scala/.../component/VeloxLanceComponent.scala` | Registers the rule when lance-spark is on the classpath |
| `backends-velox/src-lance/main/resources/META-INF/gluten-components/...VeloxLanceComponent` | Makes the component discoverable |
| `backends-velox/src-lance/test/scala/.../VeloxLanceSuite.scala` | Plan shape, results and fallback tests |
| `pom.xml`, `backends-velox/pom.xml` | `lance` / `lance-test` profiles that add `src-lance` |

### Step 1: Declare the convention

Implement a `GlutenPlan` (usually through `ValidatablePlan`) and declare what it outputs and
requires:

```scala
case class MyArrowExec(child: SparkPlan) extends UnaryExecNode with ValidatablePlan {
  override def batchType(): Convention.BatchType = ArrowJavaBatchType
  override def rowType(): Convention.RowType = Convention.RowType.None
  // The default requires the children to have this node's convention, here ArrowJavaBatchType.
  // Override it only if the operator accepts something else.
  override def requiredChildConvention(): Seq[ConventionReq] =
    Seq(ConventionReq.ofBatch(ConventionReq.BatchType.Is(ArrowJavaBatchType)))

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(s"$nodeName does not support row-based execution.")
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = ...
}
```

Don't insert `LoadArrowDataExec`, `OffloadArrowDataExec` or `ArrowColumnarToVeloxColumnarExec`
yourself. With these declarations `InsertTransitions` produces, for example:

- Velox child → your operator: `LoadArrowDataExec` is added below it.
- Your operator → Velox parent: `OffloadArrowDataExec` → `ArrowColumnarToVeloxColumnarExec` is
  added above it.
- Your operator → vanilla row parent: only `ColumnarToRowExec`, with no Velox round trip.

### Step 2: Produce batches that Gluten can own

The batches you emit are offloaded and closed by the transitions downstream, so each batch must
own its data:

- **Build the columns as `RefCountedArrowColumnVector`s** with `ArrowColumnVectors`: fill a root
  from `allocateRoot` and call `toBatch`, wrap existing `FieldVector`s with `wrap`, or write rows
  with `fromRows`. `offload` also accepts plain Spark `ArrowColumnVector`s, but has to adopt them
  first, which copies the buffers when they come from another allocator root.
- **Allocate from `ArrowBufferAllocators.contextInstance()`.** This keeps the memory accounted by
  Spark, and keeps the vectors in the allocator tree that `offload` expects.
- **Don't emit vectors that a reader reuses.** An `ArrowReader` (e.g. from `Data.importArrayStream`)
  loads every batch into the same `VectorSchemaRoot`. Lance transfers each vector out of the root
  before wrapping it (`getTransferPair(allocator).transfer()`), which moves the buffers without a
  copy and lets the batch outlive the reader.
- **Close what you emit.** Lance wraps its iterator with
  `Iterators.wrap(...).recyclePayload(_.close())`, like `ColumnarRangeExec`.
- **Release native resources early and on task end.** Close a reader as soon as it's exhausted,
  and add a `TaskContext` completion listener as a safety net for `LIMIT` and failures. Make
  `close()` idempotent.
- **Cross library boundaries with C structs only.** If the source has its own Arrow Java build,
  pass the address of the `ArrowArrayStream` / `ArrowArray` and wrap it with Gluten's Arrow
  (`ArrowArrayStream.wrap(address)`). Lance does this so that lance-spark and Gluten can use
  different Arrow versions.

For an operator with a child (e.g. a UDF operator), the input batch is valid only until the next
call on the child iterator. If you keep it longer, for example until a result comes back, call
`ColumnarBatches.retain` on it and `ColumnarBatches.release` once you're done.

### Step 3: Validate and fall back

Only offload what the operator can run. Check unsupported cases at planning time, not at run time:
once a node is offloaded, an exception fails the query instead of falling back. Lance keeps a scan
on vanilla Spark when it has a pushed aggregation or a full-text query
(`LanceScanTransformer.isExportable`). Put further checks in `doValidateInternal()`.

### Step 4: Write the offload rule

Implement `OffloadSingleNode` and inject it with a validator, so that the rule only fires for
plans that pass validation:

```scala
case class OffloadMyOperator() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case p: SomeSparkExec if MyArrowExec.isSupported(p) => MyArrowExec(p.child)
    case other => other
  }
}

object OffloadMyOperator {
  def inject(injector: Injector): Unit = {
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadMyOperator())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload)
    }
  }
}
```

### Step 5: Register a component (for optional dependencies)

If the operator depends on a library that isn't always present, register it as a `Component`:

- `dependencies()`: e.g. `classOf[VeloxBackend] :: Nil`.
- `isRuntimeCompatible`: check that the library is on the classpath (Lance checks for
  `org.lance.spark.read.LanceScan`), so the default bundle can ship the component.
- `injectRules(injector)`: call the rule's `inject`.
- List the class in `src-xxx/main/resources/META-INF/gluten-components/<class name>`.
- Put the sources under `backends-velox/src-xxx` behind a Maven profile, like `src-lance` and
  `src-iceberg`. Exclude the library's transitive Arrow so Gluten's Arrow version is kept.

### Step 6: Test

Cover at least the following, as `VeloxLanceSuite` does:

- The node is offloaded, and the plan has the expected transitions: none to a vanilla row consumer,
  and `OffloadArrowDataExec` → `ArrowColumnarToVeloxColumnarExec` to a Velox consumer.
- Results match vanilla Spark over several batches and several partitions, with nulls.
- `LIMIT` and other early stops release native resources.
- Unsupported cases fall back to vanilla Spark.

Add the profile to the CI job that runs the suite, and a page under
`docs/get-started/` if users have to enable anything.

### Checklist

- [ ] `batchType()` / `rowType()` / `requiredChildConvention()` declared; no transitions inserted
      by hand
- [ ] Columns are `RefCountedArrowColumnVector`s (`ArrowColumnVectors`) from
      `ArrowBufferAllocators.contextInstance()`
- [ ] Emitted batches own their buffers; reused reader vectors are transferred first
- [ ] Input batches kept beyond the next call are retained and released
- [ ] Resources released on exhaustion and on task completion
- [ ] Unsupported cases rejected at planning time
- [ ] Rule injected with a validator; component gated on the classpath
- [ ] Tests for plan shape, results, early stop and fallback

## Ongoing work

These change parts of the above once merged:

- Gluten's task allocator is its own `RootAllocator`, so `adopt` copies vectors that Spark
  allocated (from `ArrowUtils.rootAllocator`). Allocating Gluten's task buffers as a child of
  Spark's root allocator would turn that copy into a move.
- A DSv2 scan whose columnar reader emits Spark `ArrowColumnVector`s, such as an Arrow output mode
  in lance-spark ([discussion](https://github.com/apache/gluten/pull/12958#issuecomment-6304202853))
  or Spark's native data sources ([apache/spark#59313](https://github.com/apache/spark/pull/59313)),
  can then be offloaded without a Gluten scan operator: a rule marks its `BatchScanExec` as
  `ArrowJavaBatchType`, and `offload` adopts the batches.
- SPARK-57468 ([apache/spark#59154](https://github.com/apache/spark/pull/59154)) adds conventions to
  Spark itself, with an `ArrowBatchType`. With the prototype bridge in
  [#13140](https://github.com/apache/gluten/issues/13140), `ArrowJavaBatchType` is bound to Spark's
  `ArrowBatchType`, so a Spark operator that declares `ArrowBatchType` gets Gluten's transitions
  without a Gluten operator. Gluten's own `ColumnarArrowEvalPythonExec` and Python runner are then
  replaced by Spark's `ArrowEvalPythonExec`.
- Spark's batch ownership rules for Arrow vectors are still being settled upstream (SPARK-60294,
  SPARK-60306, SPARK-60325). Operators that keep Arrow batches across calls should follow them once
  merged.
