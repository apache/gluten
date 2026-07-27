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

| `NativeScope` | Delivery point | JVM entry | Native receiver |
|---|---|---|---|
| `BACKEND` | Once, during native backend initialization | `GlutenConfig.getNativeBackendConf` | e.g. Velox `VeloxBackend::init` (`backendConf_`), CH `BackendInitializerUtil` |
| `RUNTIME` | Each time a native runtime instance is created (per task pipeline / native memory manager) | `GlutenConfig.getNativeSessionConf` | e.g. Velox `VeloxRuntime` (`confMap_` / `veloxCfg_`) |

`NativeScope.ALL` means both. The two channels land in separate native config objects and never
merge: `backendConf_` holds only backend conf, `veloxCfg_` only session conf, and each read site
sees exactly one of them. (The one place both are visible is the Iceberg writer, which merges the
backend map underneath the session map, so the session value wins and the backend one acts as a
fallback.)

### Mutability determines the scope

The scope is not stated by the caller. It follows the conf's mutability, which is what the
declaration method already says:

- **Modifiable at any time, usable at any time** (`buildConf` / `registerConf`). Delivered on
  **both** channels, so native observes the current value wherever it reads the key.
- **Set while the native backend is initialized, not modifiable afterwards** (`buildStaticConf` /
  `registerStaticConf`). Delivered **once during native backend initialization**; a snapshot taken
  there is the value, forever.

That is the whole rule. There is no scope argument anywhere in the API, and no way for a caller to
ask for a combination that contradicts the conf's declared mutability.

Consequently, re-declaring a conf's mutability is the way to change its native delivery — and it is
a user-visible change beyond conf passing: a static conf rejects `spark.conf.set` / `SET` and is
labelled differently in the generated configuration docs. Declare a conf static only if it really
is immutable after startup.

## Declaration API

`ConfigRegistry` offers four declaration methods, split along two axes — who owns the key, and
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

val COLUMNAR_VELOX_FILE_HANDLE_CACHE_ENABLED =
  buildStaticConf("spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled")
    .passToNative()
    .passDefault()       // native relies on the key being always present
    .booleanConf
    .createWithDefault(true)
```

- `passToNative()`: registers the conf to `NativeConfRegistry` on entry creation.
- `passDefault()`: additionally delivers the conf's own default value (in parsed form, e.g. a
  "64MB" bytes conf is delivered as "67108864") when the user did not set the conf. Requires
  `passToNative()` and a defined default value; both are checked eagerly at entry creation. The
  default is re-resolved on each delivery, so an entry declared with `createWithDefaultFunction`
  keeps passing its current value.
- `nativeTransform(fn)`: normalizes a user-set value before delivery, e.g. a size string to a
  number of bytes, or upper-casing. Not applied to the value delivered by `passDefault()`, which is
  already in its final form.

The markers are available both before and after the value type is chosen, so
`.passToNative().intConf.createWithDefault(4096)` and `.intConf.passToNative().createWithDefault(4096)`
are equivalent.

### Spark / Hadoop configurations

Keys owned by Spark or Hadoop have no Gluten `ConfigEntry` and must not get one — their owner
already registered them, and registering again conflicts with it. `registerConf` /
`registerStaticConf` declare only the native delivery:

```scala
registerConf(SPARK_S3_PATH_STYLE_ACCESS)
  .doc("Read by the native S3 file system.")
  .booleanConf
  .passToNative()
  .passDefault()
  .createWithDefault(true)

registerStaticConf("spark.sql.orc.compression.codec")
  .doc("Consumed by ClickHouse backend initialization.")
  .stringConf
  .passToNative()
  .passDefault()
  .createWithDefault("snappy")
```

The default declared here is what native receives when the user did not set the key. It does not
have to repeat, and is not checked against, the owner's own default — the S3 confs above are an
example where Gluten deliberately differs from Hadoop's `core-default.xml`. Where the intent *is*
to mirror Spark's default, take it from Spark's own entry so it cannot drift across versions, and
mirror it as a *function* — a Spark default may itself be dynamic, and reading it once at
declaration time would pin whatever it resolved to while the conf object was initializing:

```scala
registerConf(SQLConf.CASE_SENSITIVE.key)
  .stringConf
  .passToNative()
  .passDefault()
  .createWithDefaultFunction(() => SQLConf.CASE_SENSITIVE.defaultValueString)

// spark.sql.session.timeZone is the case that makes this mandatory: its default is the current
// JVM default time zone, which a session (or a test) may change after this declaration has run.
registerConf(SQLConf.SESSION_LOCAL_TIMEZONE.key)
  .stringConf
  .passToNative()
  .passDefault()
  .createWithDefaultFunction(() => SQLConf.SESSION_LOCAL_TIMEZONE.defaultValueString)
```

`passToNative()` is mandatory for these: a foreign conf is not read on the JVM side, so declaring
one without delivering it to native would have no effect at all.

### Falling back to a Spark configuration

When a Gluten conf is an *override* of a Spark one — the user sets it only to depart from Spark's
choice — declare the relationship instead of hand-writing the fallback at each read site:

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

A caller that must treat the two sources differently — e.g. validating an explicitly set value
against a stricter set of allowed values — uses `readWithSource`, which returns the value together
with whether it came from the Gluten key:

```scala
val (codec, isSetOnGlutenConf) = COLUMNAR_SHUFFLE_CODEC.readWithSource(provider)
```

This is preferable to looking the key up a second time, because the value and its origin come from
one read and cannot disagree.

Note the fallback is a JVM-side notion: only a user-set value is delivered to native, which reads
the Spark key itself when the Gluten one is absent.

### Adding native confs from a backend or a component

A conf object is a Scala object, so declaring one is not enough — its registrations only happen
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
  val ACME_BATCH_SIZE =
    buildConf("spark.gluten.acme.batchSize").passToNative().intConf.createWithDefault(1024)

  val ACME_CACHE_ENABLED =
    buildStaticConf("spark.gluten.acme.cacheEnabled")
      .passToNative()
      .passDefault()
      .booleanConf
      .createWithDefault(true)

  registerConf("spark.acme.endpoint").stringConf.passToNative().createOptional
}
```

This is the only supported way for a component to get its confs into the **BACKEND** channel.
Registering from `onDriverStart` is too late: backends are root nodes of the component DAG, so a
backend's `onDriverStart` — which is where native backend initialization happens — runs before any
dependent component's. Runtime-incompatible components are skipped, so an excluded component's
confs never reach native side.

Everything above works from outside the `org.apache.gluten` package: `ConfigRegistry`,
`ConfigEntry`, `NativeConfRegistry` and `NativeScope` are public, and the four declaration methods
are `protected` members of `ConfigRegistry`. An out-of-tree backend can also call
`MyConfig.ensureRegistered()` from its `ListenerApi` instead of using `confs()`.

### Modularity

Registrations live in the conf object of the owning module (`GlutenCoreConfig`, `GlutenConfig`,
`VeloxConfig`, `CHConfig`, ...) and take effect when that object is loaded. A backend's or
connector's registrations therefore never leak into another deployment: Velox-only keys are
declared in backends-velox and simply do not exist when running the ClickHouse backend, and
vice versa — the `spark.hadoop.input.*` timeouts and `spark.sql.orc.compression.codec`, which
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
keys native depends on individually — especially those needing defaults or transforms.

Note the registry selection runs **before** the prefix rules in both methods, and the prefix
rules overwrite. A registered key that is also matched by a prefix rule therefore loses its
`nativeTransform`. No current declaration hits this (all transforms are on `spark.sql.*` /
`spark.shuffle.*` / `spark.unsafe.*` keys, none of which match a prefix rule), but a
`spark.gluten.<backend>.*` or `...backend.<backend>.*` key must not be declared with a transform.

## Confs consumed on both sides of native

A conf that native reads at backend initialization *and* per runtime is simply declared modifiable,
which delivers it on both channels. The backend-init delivery is a one-time snapshot of the startup
value, while the runtime channel follows the current value, so the two can differ; what that means
per conf:

| Conf | Backend-init consumption (frozen at startup) | Runtime consumption (follows session) |
|---|---|---|
| `spark.gluten.sql.debug` | keeps user glog levels | per-task input/plan debug dumps |
| `spark.gluten.sql.columnar.cudf` | one-time GPU environment initialization | per-query CPU/GPU offload decision. Enabling in-session without startup enablement will not initialize the GPU |
| `spark.gluten.memory.task.offHeap.size.in.bytes` | CH external sort/aggregation thresholds | Velox per-task spill memory limit |
| `spark.gluten.velox.awsSdkLogLevel`, `spark.gluten.velox.s3UseProxyFromEnv`, `spark.gluten.velox.s3PayloadSigningPolicy` | reused HiveConnector construction | re-read on each data source sink creation, see below |
| `spark.sql.legacy.statisticalAggregate`, `spark.sql.decimalOperations.allowPrecisionLoss`, `spark.sql.legacy.timeParserPolicy` | expression/aggregate behavior fixed into reused backend structures | per-query expression evaluation |
| `spark.hadoop.fs.s3a.*` connection confs (ssl, path-style, retry attempts, connection maximum, ...) | reused HiveConnector construction | per-query file system access, see below |

### `createHiveConnectorConfig` runs on both channels

Velox's `createHiveConnectorConfig` is not backend-init-only. Besides building the reused
`HiveConnector` from the backend conf (`VeloxBackend::init`), it is called per write from the
**runtime** conf map, with no backend fallback merged in — see `VeloxParquetDataSourceS3::initSink`
and friends, and `IcebergWriter`. Any conf it reads must therefore be declared modifiable so it
reaches the runtime channel too, which is why the three `spark.gluten.velox.*` S3 confs above are
not static.

Native has its own fallback for the `spark.hadoop.fs.s3a.*` connection confs, and it does not
always agree with what Gluten declares — `path.style.access` falls back to `false` in
`ConfigExtractor` while Gluten declares `true`. Declaring the default makes Gluten's value the one
native sees on both channels, which is the intent; the previous behavior, where the write path
silently got a different default from the read path, was a latent inconsistency rather than a
contract.

### Confs kept modifiable for JVM-side reasons

`GlutenAutoAdjustStageResourceProfile.updateResourceSetting` rewrites three confs on `SQLConf`
per stage when a new resource profile is applied, and JVM-side readers observe the rewritten
values:

- `spark.gluten.numTaskSlotsPerExecutor` — native reads it at backend init only (Velox io/spill
  thread sizing), and warns and falls back to 1 when the key is missing, hence `passDefault()`.
- `spark.gluten.memory.offHeap.size.in.bytes` — native reads it at backend init only (CH spill
  thresholds).
- `spark.gluten.memory.task.offHeap.size.in.bytes` — read on both sides, see the table above.

These stay modifiable because they are genuinely session-mutable, not because a static declaration
would break the rewrite: `SQLConf.setConfString` does not reject static keys (that guard lives in
`RuntimeConfig.set` and `SET`). Declaring them static would assert an immutability that does not
hold, break `SET` on them, and mislabel them in the generated configuration docs.

Backend note: the ClickHouse backend calls `getNativeBackendConf` both at `initNative` and per
kernel pipeline, so for CH the BACKEND scope effectively refreshes per query as well; the
Velox backend consumes BACKEND scope strictly once at init. The scope contract above is
defined by the stricter (Velox) behavior.

## Object initialization

A module's registrations only exist once its conf object is initialized. Since a reference to a
constant `val` can be folded away by the compiler, forcing initialization by touching a field is
not reliable; use `ConfigRegistry.ensureRegistered()`, a no-arg method that always triggers
initialization.

The initialization points, in the order they run:

1. `Component.confs()` — Gluten calls `ensureRegistered()` on every registered component's conf
   objects right after component discovery, before any `onDriverStart` / `onExecutorStart`. This is
   the recommended hook, and the only one early enough for the BACKEND channel. `VeloxBackend` and
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
  per-channel scope, defaults and value normalization.
- `GlutenConfigUtil.mapByteConfValue` (superseded by `nativeTransform`).
- `spark.gluten.velox.fs.s3a.retry.mode`, which had no native reader: native reads the retry mode
  from `spark.hadoop.fs.s3a.retry.mode`, and the gluten-namespaced key was orphaned when the S3
  config path moved to velox's `S3Config`.
- The hand-written fallback from `spark.gluten.sql.columnar.shuffle.codec` to
  `spark.io.compression.codec` in `GlutenShuffleUtils`, now expressed by `fallbackConf`.

## Testing

- `org.apache.gluten.config.NativeConfRegistrySuite` (gluten-core) covers the declaration API:
  which channel each of the four methods delivers on, `passDefault` in parsed form and its
  constraint checks, `nativeTransform`, Spark fallback resolution, and duplicate declaration.
- `org.apache.gluten.config.NativeConfPassingSuite` (gluten-substrait) covers the delivered
  result end to end: what `getNativeSessionConf` / `getNativeBackendConf` actually select,
  including byte-string normalization, per-channel scoping and the always-present defaults.
- `org.apache.gluten.config.ShuffleCodecConfSuite` (gluten-substrait) covers the shuffle codec's
  fallback resolution and its reported origin.
- `org.apache.gluten.component.ComponentSuite` (gluten-core) covers the `Component.confs()` hook:
  that it defaults to empty, and that initializing a component's conf objects registers their
  native confs into the expected channels.
