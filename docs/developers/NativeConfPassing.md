---
layout: page
title: Passing Configurations from JVM to Native
nav_order: 19
parent: Developer Overview
---

# Passing Configurations from JVM to Native

This document describes the design of Gluten's JVM-to-native configuration-passing mechanism,
built around `ConfigRegistry`'s declaration methods and `ConfigBuilder.passToNative()`.

## Background and motivation

Native code (Velox / ClickHouse backends) consumes Spark, Hadoop and Gluten configurations.
Historically, the JVM side decided which keys to deliver via hard-coded string lists inside
`GlutenConfig.getNativeSessionConf` / `getNativeBackendConf`:

- A `nativeKeys` set of 40+ raw string keys, including backend-specific keys living in common
  code (e.g. Velox-only S3 keys in gluten-substrait).
- Two "configs having default values" `Seq`s duplicating each conf's key and default.
- Ad-hoc per-key special cases (byte-unit conversion, upper-casing, etc.) inline in the
  selection methods.

This was hard to maintain: adding a native conf required editing central lists far away from
the conf's definition, defaults were duplicated and could drift, and backend-specific keys
leaked into common code.

## Core model

### Two delivery channels

There are exactly two points where the JVM delivers confs to native, matching the two lifecycle
stages of a native backend:

| Channel | Delivery point | JVM entry | Native receiver |
|---|---|---|---|
| backend | Once, during native backend initialization | `GlutenConfig.getNativeBackendConf` | e.g. Velox `VeloxBackend::init` (`backendConf_`), CH `BackendInitializerUtil` |
| runtime | Each time a native runtime instance is created (per task pipeline / native memory manager) | `GlutenConfig.getNativeSessionConf` | e.g. Velox `VeloxRuntime` (`confMap_` / `veloxCfg_`) |

A modifiable conf lands on both. The two channels land in separate native config objects and never
merge: `backendConf_` holds only backend conf, `veloxCfg_` only session conf, and each read site
sees exactly one of them. (The one place both are visible is the Iceberg writer, which merges the
backend map underneath the session map, so the session value wins and the backend one acts as a
fallback.)

Because the backend channel is delivered exactly once, a conf object that initializes *after* that
delivery can only reach the runtime channel. `NativeConfRegistry` latches the backend channel on
first delivery and logs a warning naming any key declared afterwards, so the gap is reported rather
than silently half-applied. Declaring the conf object through `Component.confs()` (below) is what
avoids it.

### Mutability determines the channels

The scope is not stated by the caller. It follows the conf's mutability, which is what the
declaration method already says:

- **Modifiable at any time, usable at any time** (`buildConf` / `registerConf`). Delivered on
  **both** channels, so native observes the current value wherever it reads the key.
- **Set while the native backend is initialized, not modifiable afterwards** (`buildStaticConf` /
  `registerStaticConf`). Delivered **once during native backend initialization**; a snapshot taken
  there is the value, forever.

That is the whole rule. There is no channel argument anywhere in the API, and no way for a caller to
ask for a combination that contradicts the conf's declared mutability.

Consequently, re-declaring a conf's mutability is the way to change its native delivery - and it is
a user-visible change beyond conf passing: a static conf rejects `spark.conf.set` / `SET` and is
labelled differently in the generated configuration docs. Declare a conf static only if it really
is immutable after startup.

## Declaration API

`ConfigRegistry` offers four declaration methods, split along two axes - who owns the key, and
whether it is modifiable:

| | Modifiable at any time | Set at backend init, then immutable |
|---|---|---|
| Owned by Gluten | `buildConf` | `buildStaticConf` |
| Owned by Spark / Hadoop | `registerConf` | `registerStaticConf` |

### Gluten's own configurations

```scala
val COLUMNAR_MAX_BATCH_SIZE =
  buildConf("spark.gluten.sql.columnar.maxBatchSize")
    .passToNative()
    .intConf
    .createWithDefault(4096)
```

- `passToNative()`: registers the conf to `NativeConfRegistry` on entry creation. A value set by the
  user is delivered through the conf's own value converter - the one chosen by `stringConf` /
  `bytesConf(unit)` / `intConf` / ... plus any `transform` - so a "64MB" bytes conf reaches native
  as "67108864", and a `timeParserPolicy` conf reaches native upper-cased even when the user wrote
  it in lower case. When the user did not set it, the conf's declared default is delivered instead,
  in the same converted form.

The value conversion is stated once, at the declaration, by the type builder and any `transform`.
There is no separate "normalize before delivery" step: `bytesConf(ByteUnit.KiB)` yields the KiB
count Spark's own entry would yield (native applies any further unit conversion it needs, e.g.
`spark.shuffle.file.buffer` is KiB on both sides and native multiplies by 1024), and
`bytesConf(ByteUnit.BYTE)` yields a byte count directly. A foreign conf declares the same converter
its owner declares, so JVM and native agree on the value's meaning.

The default is read per delivery rather than snapshotted at declaration, so an entry declared with
`createWithDefaultFunction` keeps delivering its current value.

#### When a conf should have no default

The conf's own declaration decides what an unset key delivers. `createOptional` means "deliver only
when set", which hands the decision to native's own fallback - and that is the right choice whenever
native has a fallback that is already correct, since declaring a redundant default only makes the JVM
a second owner of the same value, to be kept in step by hand.

More importantly, some native read sites branch on whether the key is *present*, not on its value:

```cpp
if (!backendConf_->valueExists(kNumTaskSlotsPerExecutor)) { /* warn, fall back to 1 */ }
if (conf->valueExists(sparkKey)) { /* ... */ }              // ConfigExtractor.cc, S3 keys
GLUTEN_CHECK(saveDir.has_value(), kGlutenSaveDir + " is not set");
```

For such a key, a default would defeat the check - so it must be declared `createOptional`. This is
why `spark.gluten.numTaskSlotsPerExecutor` and `spark.gluten.saveDir` have no default: their value
cannot be derived at declaration time, and a placeholder (`-1`, `""`) would pass native's presence
check and then fail its validation, or be used as a real value.

Conversely, declare a default when leaving the key out would change behavior:

- **native has no fallback and fails without the key** -
  `spark.gluten.sql.columnarToRowMemoryThreshold` (`GLUTEN_CHECK` in `JniWrapper.cc`) and
  `spark.gluten.memory.backtrace.allocation` (`std::unordered_map::at`) both throw when it is absent;
- **native's fallback disagrees with what Gluten wants** - `fs.s3a.path.style.access` falls back to
  `false` in `ConfigExtractor` where Gluten wants `true`.

The markers are available both before and after the value type is chosen, so
`.passToNative().intConf.createWithDefault(4096)` and `.intConf.passToNative().createWithDefault(4096)`
are equivalent.

### Spark / Hadoop configurations

Keys owned by Spark or Hadoop have no Gluten `ConfigEntry` and must not get one - their owner
already registered them, and registering again conflicts with it. `registerConf` /
`registerStaticConf` declare only the native delivery:

```scala
registerConf(SQLConf.CASE_SENSITIVE.key).booleanConf.passToNative().createOptional

registerConf(SQLConf.SESSION_LOCAL_TIMEZONE.key).stringConf.passToNative().createWithForeignDefault

registerConf(SPARK_S3_PATH_STYLE_ACCESS)
  .doc("Read by the native S3 file system.")
  .booleanConf
  .passToNative()
  .createWithDefault(true)
```

The terminal method states what is delivered when the user did not set the key:

- `createOptional`: nothing is delivered. Native's own fallback applies. This is the common case -
  native usually declares the same fallback Spark/Hadoop does, or branches on the key being absent.
- `createWithForeignDefault`: delivers the default Spark/Hadoop declares for the key,
  resolved freshly at each delivery. Use it only when native's fallback is wrong or missing and the
  foreign default is dynamic or version-dependent - `spark.sql.session.timeZone` follows the JVM
  default time zone, and `spark.sql.ansi.enabled` flipped its default in Spark 4.0. Never restate
  such a default on the Gluten side - that is exactly what drifts.
- `createWithDefault(value)`: delivers Gluten's own chosen value. Use it only when Gluten
  deliberately departs from what both Spark/Hadoop and native would apply -
  `path.style.access` above differs from both Hadoop's `core-default.xml` and
  `ConfigExtractor`'s `false`.

A Hadoop key that no Spark entry declares (`spark.hadoop.fs.s3a.access.key`, ...) resolves to no
default under `createWithForeignDefault` either, so in practice it behaves identically to
`createOptional` for such keys. Use `createOptional` for them - it is more explicit about the
intent.

`passToNative()` is mandatory for these: a foreign conf is not read on the JVM side, so declaring
one without delivering it to native would have no effect at all.

### Falling back to a Spark configuration

When a Gluten conf is an *override* of a Spark one - the user sets it only to depart from Spark's
choice - declare the relationship instead of hand-writing the fallback at each read site:

```scala
val COLUMNAR_SHUFFLE_CODEC =
  buildConf("spark.gluten.sql.columnar.shuffle.codec")
    .doc(s"The codec used for columnar shuffle compression. Defaults to $SPARK_IO_COMPRESSION_CODEC.")
    .stringConf
    .transform(_.toLowerCase(Locale.ROOT))
    .passToNative()
    .fallbackConf(SPARK_IO_COMPRESSION_CODEC, SPARK_IO_COMPRESSION_CODEC_DEFAULT)
```

Reading the entry yields the Gluten value if set, else the Spark value if set, else Spark's
default, so callers never write a `None` branch. The fallback is stated by key and default value
rather than as Spark's `ConfigEntry`, which is `private[spark]` and cannot appear in a signature
outside `org.apache.spark`.

A caller that must treat the two sources differently - e.g. validating an explicitly set value
against a stricter set of allowed values - uses `readWithSource`, which returns the value together
with whether it came from the Gluten key:

```scala
val (codec, isSetOnGlutenConf) = COLUMNAR_SHUFFLE_CODEC.readWithSource(provider)
```

This is preferable to looking the key up a second time, because the value and its origin come from
one read and cannot disagree.

Note the fallback is a JVM-side notion: only a user-set value is delivered to native, which reads
the Spark key itself when the Gluten one is absent.

### Adding native confs from a backend or a component

A conf object is a Scala object, so declaring one is not enough - its registrations only happen
once something touches it. Declare the object through `Component.confs()` and Gluten initializes
it, right after component discovery and before any component's `onDriverStart` /
`onExecutorStart`:

```scala
class AcmeComponent extends Component {
  override def name(): String = "acme"
  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil
  override def confs(): Seq[ConfigRegistry] = Seq(AcmeConfig)
  override def injectRules(injector: Injector): Unit = ...
}

object AcmeConfig extends ConfigRegistry {
  override def get: GlutenCoreConfig = GlutenCoreConfig.get

  val ACME_BATCH_SIZE =
    buildConf("spark.gluten.acme.batchSize").passToNative().intConf.createWithDefault(1024)

  val ACME_CACHE_ENABLED =
    buildStaticConf("spark.gluten.acme.cacheEnabled")
      .passToNative()
      .booleanConf
      .createWithDefault(true)

  registerConf("spark.acme.endpoint").stringConf.passToNative().createOptional
}
```

This is the only supported way for a component to get its confs into the **backend** channel.
Registering from `onDriverStart` is too late: backends are root nodes of the component DAG, so a
backend's `onDriverStart` - which is where native backend initialization happens - runs before any
dependent component's. Runtime-incompatible components are skipped, so an excluded component's
confs never reach native side.

Everything above works from outside the `org.apache.gluten` package: `ConfigRegistry`,
`ConfigEntry` and `NativeConfRegistry` are public, and the four declaration methods
are `protected` members of `ConfigRegistry`. An out-of-tree backend can also call
`MyConfig.ensureRegistered()` from its `ListenerApi` instead of using `confs()`.

### Modularity

Registrations live in the conf object of the owning module (`GlutenCoreConfig`, `GlutenConfig`,
`VeloxConfig`, `CHConfig`, ...) and take effect when that object is loaded. A backend's or
connector's registrations therefore never leak into another deployment: Velox-only keys are
declared in backends-velox and simply do not exist when running the ClickHouse backend, and
vice versa - the `spark.hadoop.input.*` timeouts and `spark.sql.orc.compression.codec`, which
only `CHTransformerApi` consumes, are declared in `CHConfig`.

## Selection at delivery time

- `getNativeSessionConf` = `NativeConfRegistry.selectRuntimeConf(sessionConf)` + prefix rules
  (`spark.gluten.sql.columnar.backend.<backend>` for non-static keys,
  `spark.gluten.<backend>`) + UGI tokens.
- `getNativeBackendConf` = `NativeConfRegistry.selectBackendConf(conf)` + prefix rules
  (`spark.gluten.sql.columnar.backend.<backend>`, `spark.hadoop.fs.s3a.` / `fs.azure.` /
  `fs.gs.`, `spark.gluten.<backend>`).

Prefix rules are pattern-based and intentionally kept: they cover open-ended key families
(e.g. arbitrary `fs.s3a` client options) that cannot be enumerated. The registry covers the
keys native depends on individually - especially those needing defaults or transforms.

Note the registry selection runs **before** the prefix rules in both methods, and `getOrElseUpdate`
is used rather than `put` so a registry-selected value wins: overwriting it with the raw prefix
value would bypass the entry's value converter for a registered key that also matches a prefix
rule.

## Confs consumed on both sides of native

A conf that native reads at backend initialization *and* per runtime is simply declared modifiable,
which delivers it on both channels. The backend-init delivery is a one-time snapshot of the startup
value, while the runtime channel follows the current value, so the two can differ; what that means
per conf:

| Conf | Backend-init consumption (frozen at startup) | Runtime consumption (follows session) |
|---|---|---|
| `spark.gluten.sql.debug` | keeps user glog levels | per-task input/plan debug dumps |
| `spark.gluten.sql.columnar.cudf` | one-time GPU environment initialization | per-query CPU/GPU offload decision. Enabling in-session without startup enablement will not initialize the GPU |
| `spark.gluten.memory.task.offHeap.size.in.bytes` | CH external sort/aggregation thresholds | Velox partial-aggregation memory limits |
| `spark.gluten.velox.awsSdkLogLevel`, `spark.gluten.velox.s3UseProxyFromEnv`, `spark.gluten.velox.s3PayloadSigningPolicy` | reused HiveConnector construction | re-read on each data source sink creation, see below |
| `spark.sql.legacy.statisticalAggregate`, `spark.sql.decimalOperations.allowPrecisionLoss`, `spark.sql.legacy.timeParserPolicy` | expression/aggregate behavior fixed into reused backend structures | per-query expression evaluation |
| `spark.hadoop.fs.s3a.*` connection confs (ssl, path-style, retry attempts, connection maximum, ...) | reused HiveConnector construction | per-query file system access, see below |

### `createHiveConnectorConfig` runs on both channels

Velox's `createHiveConnectorConfig` is not backend-init-only. Besides building the reused
`HiveConnector` from the backend conf (`VeloxBackend::init`), it is called per write from the
**runtime** conf map, with no backend fallback merged in - see `VeloxParquetDataSourceS3::initSink`
and friends, and `IcebergWriter`. Any conf it reads must therefore be declared modifiable so it
reaches the runtime channel too, which is why the three `spark.gluten.velox.*` S3 confs above are
not static.

No Spark entry declares the `spark.hadoop.fs.s3a.*` connection confs, so an unset one resolves to no
default and native's own fallback applies. Three of them declare a Gluten-side default because
Gluten's choice departs from that fallback - `path.style.access` falls back to `false` in
`ConfigExtractor` while Gluten wants `true`, `connection.maximum` to `25` while Gluten wants `15`, and
`retry.limit` has no native fallback at all - which makes Gluten's value the one native sees on both
channels. The previous behavior, where the write path silently got a different default from the read
path, was a latent inconsistency rather than a contract.

### Confs kept modifiable for JVM-side reasons

`GlutenAutoAdjustStageResourceProfile.updateResourceSetting` rewrites three confs on `SQLConf`
per stage when a new resource profile is applied, and JVM-side readers observe the rewritten
values:

- `spark.gluten.numTaskSlotsPerExecutor` - native reads it at backend init only (Velox io/spill
  thread sizing), and warns and falls back to 1 when the key is missing.
- `spark.gluten.memory.offHeap.size.in.bytes` - no native reader (the key is declared in
  `cpp/core/config/GlutenConfig.h` but read nowhere); the ClickHouse backend reads it JVM-side off
  the conf map. Not declared `passToNative()`.
- `spark.gluten.memory.task.offHeap.size.in.bytes` - read on both sides, see the table above.

The first and last are declared `createOptional`, because the value cannot be derived without a
SparkConf at hand and every candidate placeholder is one native would take literally: `-1` fails
`GLUTEN_CHECK(numTaskSlotsPerExecutor >= 0)`, and `0` for task off-heap collapses the
partial-aggregation limits where native's absent-key fallback is `kMaxMemory`. The same reasoning
applies to `spark.gluten.memoryOverhead.size.in.bytes` (static, so not in this list): its `0` would
have built the Velox global memory manager with zero capacity instead of taking
`VeloxBackend::init`'s `kMaxMemory` branch.

These stay modifiable because they are genuinely session-mutable, not because a static declaration
would break the rewrite: `SQLConf.setConfString` does not reject static keys (that guard lives in
`RuntimeConfig.set` and `SET`). Declaring them static would assert an immutability that does not
hold, break `SET` on them, and mislabel them in the generated configuration docs.

Backend note: the ClickHouse backend calls `getNativeBackendConf` both at `initNative` and per
kernel pipeline, so for CH the backend channel effectively refreshes per query as well; the
Velox backend consumes it strictly once at init. The contract above is defined by the stricter
(Velox) behavior.

## Object initialization

A module's registrations only exist once its conf object is initialized. Since a reference to a
constant `val` can be folded away by the compiler, forcing initialization by touching a field is
not reliable; use `ConfigRegistry.ensureRegistered()`, a no-arg method that always triggers
initialization.

The initialization points, in the order they run:

1. `Component.confs()` - Gluten calls `ensureRegistered()` on every registered component's conf
   objects right after component discovery, before any `onDriverStart` / `onExecutorStart`. This is
   the recommended hook, and the only one early enough for the backend channel. `VeloxBackend` and
   `CHBackend` declare `VeloxConfig` / `CHConfig` through it.
2. Explicit calls for special cases: `GlutenConfig` calls `GlutenCoreConfig.ensureRegistered()`
   before its own registrations, and `VeloxListenerApi.parseConf` / `CHListenerApi` call their
   backend conf object's `ensureRegistered()` as a belt-and-braces measure for code paths that
   reach native conf selection without going through component discovery (e.g. tests and tools).

## What was removed

- `nativeKeys` hard-coded string set (including Velox keys in common code, now declared in
  `VeloxConfig`).
- The two default-value `Seq`s and all per-key special cases in `getNativeSessionConf` /
  `getNativeBackendConf`.
- `BackendSettingsApi.extraNativeSessionConfKeys` / `extraNativeBackendConfKeys`. These were
  added shortly before this change and never overridden by any backend; the declaration API
  supersedes them, and an out-of-tree backend migrates by declaring its conf object through
  `Component.confs()`. Unlike the removed `Set[String]` hooks, a declaration can express
  per-channel delivery, defaults and value normalization.
- `GlutenConfigUtil.mapByteConfValue` (superseded by declaring `bytesConf(unit)` at the conf's
  definition, so the conversion happens through the entry's own value converter).
- The `shuffleFileBufferSize` JNI argument of `LocalPartitionWriterJniWrapper`.
  `spark.shuffle.file.buffer` is now declared with `passToNative()` and reaches native through the
  runtime conf map, where `createPartitionWriter` reads it and converts KiB to bytes. Passing it as
  a JNI argument used to hand native the KiB count as if it were a byte count, making the buffer
  1024x smaller than native's own default.
- `spark.gluten.velox.fs.s3a.retry.mode`, which had no native reader: native reads the retry mode
  from `spark.hadoop.fs.s3a.retry.mode`, and the gluten-namespaced key was orphaned when the S3
  config path moved to velox's `S3Config`.
- The hand-written fallback from `spark.gluten.sql.columnar.shuffle.codec` to
  `spark.io.compression.codec` in `GlutenShuffleUtils`, now expressed by `fallbackConf`.
- The restating of Spark defaults on the Gluten side. The old "configs having default values" lists
  spelled out each Spark default next to its key; a Spark-owned conf now declares `createOptional` and
  its default is resolved from Spark's own entry at delivery time, so the two cannot drift.
  `spark.gluten.numTaskSlotsPerExecutor` and `spark.gluten.saveDir` become `createOptional` in the
  process: their `-1` / `""` defaults were placeholders that native either rejects
  (`GLUTEN_CHECK(numTaskSlotsPerExecutor >= 0)`) or would take for a real value
  (`enableDumping` only checks the key is present).

## Testing

- `org.apache.gluten.config.NativeConfRegistrySuite` (gluten-core) covers the declaration API:
  which channel each of the four methods delivers on, the declared default in converted form and its
  re-resolution per delivery, delivery through the conf's own value converter (a `transform` and a
  `bytesConf(unit)`) over both a user-set value and a resolved default, Spark fallback resolution,
  and duplicate declaration.
- `org.apache.gluten.config.NativeConfPassingSuite` (gluten-substrait) covers the delivered
  result end to end: what `getNativeSessionConf` / `getNativeBackendConf` actually select,
  including byte-conf unit handling, per-channel scoping, where the default comes from (Gluten's
  own, Spark's declaration, or nowhere), the keys that are delivered only when set, and a declared
  key winning over an overlapping prefix rule.
- `org.apache.gluten.config.ShuffleCodecConfSuite` (gluten-substrait) covers the shuffle codec's
  fallback resolution and its reported origin.
- `org.apache.gluten.component.ComponentSuite` (gluten-core) covers the `Component.confs()` hook:
  that it defaults to empty, and that initializing a component's conf objects registers their
  native confs into the expected channels.
