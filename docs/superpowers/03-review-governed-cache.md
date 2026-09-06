# Code Review 记录 —— governed Velox cache capacity（`907fd17d` / `280d284d`）

> 审查范围:正向 feature diff `280d284d..907fd17d`（`[GLUTEN][VL] Bound the Velox memCache by memory Spark can spare`，16 文件 +1387 行）。
> 审查维度（用户指定）:1 正确性/容量受限；2 热路径锁；3 重复/dead code/多余配置；4 fast-fail 配置/可 disable；5 与 `DynamicOffHeapSizingMemoryTarget` 兼容；6 风格（Scala 对齐 Spark、C++ 对齐 velox）。
> 状态:**仅记录，未改任何代码。** 每条含定位、判断、建议改法、以及用户裁决（待填/已填）。

## 模型速览（供后续接手理解）

- `SparkMmapAllocator`（C++）:自定义 allocator，cache 容量可动态调，上限 = `staticCapacity`（memCacheSize 配置）。
- `CacheStorageReservation`（JVM，`org.apache.spark.memory`）:向 Spark storage 池 `borrow`/`repay`，基于 bare `MemoryTargets.global()`，**故意不用 `GlobalOffHeapMemory`**（后者 `throwOnOom` 会让查询崩）。
- `CacheCapacityGovernor`（JVM，`org.apache.gluten.memory`）:专用线程周期驱动；**先向 Spark 预留成功才涨 cache 容量**（`grow`），Spark 紧张时降容量并 `repay`（`shrink`/`stop`）。不变式:**预留 ≥ 容量 ≥ 实际用量**。
- JNI（`VeloxCacheJniWrapper`）:`getCacheUsedBytes`（dead）、`getCacheStaticCapacity`、`getCacheCapacity`、`adjustCacheCapacity`。

---

## 用户的 4 个发现

### 发现 1 —— `CacheCapacityGovernor` 命名 → 改名 `PeriodicMemoryChecker`
- **裁决:改名 `CacheCapacityGovernor` → `PeriodicMemoryChecker`（用户定，覆盖先前"保留"）。** 纯改名，不动逻辑。
- 范围:class + 伴生 object（`CacheCapacityGovernor.scala` → `PeriodicMemoryChecker.scala`）；线程名 `gluten-velox-cache-governor` 可一并改 `gluten-velox-cache-checker`（可选）；引用处 `VeloxListenerApi.scala`（import + `.start`/`.stop`）；测试 `CacheCapacityGovernorSuite.scala` → `PeriodicMemoryCheckerSuite.scala`（含类名）。
- 待执行。

### 发现 2 —— `getCacheUsedBytes()` dead code
- 定位:声明 `VeloxCacheJniWrapper.java:30`；JNI 实现 `VeloxJniWrapper.cc:1306-1318`。
- 核实:**JVM 侧零调用**（grep 无）；governor 全程用 `getCacheCapacity`/`adjustCacheCapacity`，从不读实际 used。
- ⚠ 边界:底层 `SparkMmapAllocator::allocatedBytes()` **不可删**——`SparkMmapAllocator.cc:33`、`VeloxJniWrapper.cc:1361`（adjustCacheCapacity）、test 都在用。**只删 `getCacheUsedBytes` 那一层**（Java 声明 + JNI 实现），保留 `allocatedBytes()`。
- **裁决:同意删（仅 getCacheUsedBytes 层）。** 待执行。

### 发现 3 —— `getCacheStaticCapacity()` C++/Java 都从配置拿（冗余）
- 定位:Java JNI `getCacheStaticCapacity()`（`CacheCapacityGovernor.scala:248`）；C++ `SparkMmapAllocator` 持 `staticCapacityBytes_ = MmapAllocator::capacity()`（memCacheSize 配置）。
- 核实链路:`memCacheSize`(配置) → `options.capacity`（`VeloxBackend.cc:371`）→ `SparkMmapAllocator` → `MmapAllocator::capacity()` → `staticCapacityBytes_`（`SparkMmapAllocator.h:42`）→ JNI 返回。**数值一路无变换，JNI 值 == 配置原值 `COLUMNAR_VELOX_MEM_CACHE_SIZE`。**
- **裁决:删走配置。** Java 改直接读 `conf.get(COLUMNAR_VELOX_MEM_CACHE_SIZE)`。
- 删除范围:Java 声明 + JNI 实现（`VeloxJniWrapper.cc:1320-1329`）。**C++ `SparkMmapAllocator::staticCapacity()` getter 保留**（test `SparkMmapAllocatorTest.cc:49,124` 在用）。
- 连带改动:`CacheCapacityGovernor.scala:248` → 读配置；gate `:249-252`、`minCacheBytes`/`maxCacheBytes`（`:267-268`）改用配置值。
- ⚠ 记录副作用:gate 从「native 真有 cache」变为「配置写了 size」，丢失「配置开但 native cache 初始化失败」的防护；由前置 `COLUMNAR_VELOX_CACHE_ENABLED` gate（`:243`）大部分覆盖，可接受。
- 待执行。

### 发现 4 —— `getCacheCapacity()` / 预留累积泄漏（reserved 无 JVM 权威记录）★ 用户定位
- 症状:`LEAK: over-reserved by 148MB (reserved=420MB capacity=272MB, gap appeared at 380MB)`；跑 40 tick 后 `reserved=420MB` 而 `capacity=272MB`，reserved 甚至超过初始 400MB，每次 pin 收缩再加一笔。方向保守（多占不 OOM）但纯浪费且累积。
- 根因（用户定位，已核实）:governor **没有 JVM 侧的 `reserved` 记录**（确认:`CacheCapacityGovernor` 只有 `stopped`/`scheduler` 字段，`borrow`/`repay` 4 处均即时调用无累计）。每 tick 从 `getCacheCapacity()` 重推。正常路径两者同步；**pin 场景岔开**:
  1. tick 收缩到 governed=200MB，380MB 被 pin → `capacity()=max(governed,allocated)=380MB`，只归还实际让出部分 → reserved=380MB ✅ 相等。
  2. reader 结束，**其他查询分配触发 native `makeSpace` 自动淘汰到 governed → `capacity()` 掉到 200MB。这步不经 governor，没人告诉 JVM。**
  3. 下一 tick 以 `capacity()=200MB` 为基准算收缩，只归还相对 200MB 的部分 → 那 180MB 差额永远回不去。
- 关键约束（不能简单删/改）:pin 住时 native 做不到要求的收缩。若 JVM 单方面把 reserved 降到目标 → `reserved < capacity`（用 700MB 只登记 200MB）= 危险方向（会 OOM）。**必须回读 `capacity()` 才知真正让出多少。** 所以两值都要，主次:**`reserved` 权威（Spark 借了多少），`capacity()` 是核对手段。**
- 注（与我早先怀疑的区分）:`getCacheCapacity` 返回 `governedCapacity()`（`VeloxJniWrapper.cc:1340`）而非 clamp 后的 `capacity()`——但根因不在读哪个值，而在缺 JVM 权威 reserved + native 自动淘汰不通知 JVM。
- **裁决:要动。** 修法:governor 新增 `var reserved: Long` 权威字段；每 tick 开头对账:
  ```scala
  val actual = capacityBytes()
  if (reserved > actual) {
    reservation.repay(reserved - actual)   // 把 native 自动淘汰腾出的还回去
    reserved = actual
  }
  ```
  对完账两者相等，后续照旧。`stop()` 用精确 `reserved` 而非再推导。此修法同时消解 #5（stop pinned 泄漏）。
- 待执行。

---

## Review 8 条发现（已独立验证；驳回项见末尾）

### 🔴 #1 CONFIRMED（降级为注释问题）—— `CacheStorageReservation.borrow` 理论上可 evict 用户块
- 定位:`CacheStorageReservation.scala:47`（注释）vs `borrow`（`GlobalOffHeapMemoryTarget.scala:60` 底层）。
- 事实:`borrow → global().borrow → mm.acquireStorageMemory → StorageMemoryPool.acquireMemory`，池满时 `evictBlocksToFreeSpace`，理论上可驱逐用户 persist 的 RDD/broadcast。
- **裁决:接受行为，只改注释。** 理由（急减慢升自限）:`nextCapacity`（`CacheCapacityGovernor.scala:78-90`）—— cache 仅在 `current < cacheStorageRatio × maxStorage`（默认 0.3）时才 grow，且 grow 是乘性小步慢升（`step = current × growRatio`）；一旦 `underPressure` 立刻乘性急减让路（`execution has first claim`，`:75-76`）。用户块多→storage 池紧张→cache 大概率已在急减区、不在借。即使边界偶发 evict 也是一小块且下 tick 立即吐回。故驱逐是极边缘、自限、自我纠正，非系统性抢占。
- 改法:软化 `CacheStorageReservation` 注释——去掉「does not preempt / only draws on the free part」的绝对措辞，改成「优先用 free 部分；极端边界可能 evict storage 块，但急减慢升令其自限且随即让路」。**不改行为。**
- 待执行。

### 🔴 #2 CONFIRMED —— `borrow` 全有或全无，partial-grant 逻辑生产永不触发
- 定位:`grow`（`CacheCapacityGovernor.scala:121`）、`start`（`:159-162`）假设 `0 < granted < wanted`；底层 `acquireStorageMemory` 返回 Boolean → borrow 只返 `wanted` 或 `0`。
- 后果:要 100MB 只剩 60MB → 返 0 → cache 不涨 → 下 tick 再要同样 100MB，卡死直到整块空出。丧失「渐进长进部分可用内存」。**单测 `FakeReservation` 会 partial-grant → 测试走的分支与生产不同，此分歧完全没测到（测试撒谎）。**
- **裁决:(a) 接受 all-or-nothing。** 靠慢升小步（`step = current × growRatio`）兜底：小步空转一 tick 无所谓，下 tick 再试。
- 改法:(1) 删 `grow`（`:121-127`）/`start`（`:159-162`）里处理「部分授予」的死逻辑，简化为「借到→按 wanted 涨；借不到→等下 tick」；(2) 改单测 `FakeReservation` 为 all-or-nothing（返 wanted 或 0），对齐生产、别再测生产走不到的路。**不引入 partial 重试（那是方案 b，已否）。**
- 待执行。

### 🟡 #3 —— `start()` 在 try/NonFatal 之外调 JNI（★ review 建议被驳回）
- 定位:`onTick`（`:92`）有 NonFatal；`object.start`（`:248`）、`class.start`（`:159` borrow）无。
- review 原建议:包 try/NonFatal 降级为「无 pushback」。
- **裁决:驳回 review，保持 start fail-fast。** 理由:pushback 开启后若配置错误 / 预留不到内存（`reservation.borrow` 拿不到），是用户/部署错误，静默降级会让用户误以为 pushback 在跑（埋雷）。**应让 executor 起不来，逼用户改配置。** 异常逃出 `onExecutorStart` 正是想要的 fail-fast 行为。
- 划分:`start`（启动期）fail-fast 不包 try；`onTick`（运行期，专用线程，抛了只是该 tick 不调整、executor 不挂）保持现有 NonFatal 容错。
- 注:发现 3 已把 `:248` `getCacheStaticCapacity()` 换读配置，那处 JNI 风险自然消失；`:159` borrow 等仍可抛，但按本裁决**有意**让其 fail-fast。
- 无需改动（维持现状）。

### 🟡 #4 —— `minCacheBytes` 地板未对 `taskSlots × loadQuantum` 校验（启动 fast-fail）
- 定位:`minCacheBytes`（默认 512MB，`CacheCapacityGovernor.scala:266`；配置 `VeloxConfig.scala:182`）。
- 约束（minCacheSize doc `:185-187` 自述）:cache < `并发readers × loadQuantum` → 只能装 pinned entry，分配不满足 → `NO_CACHE_SPACE` 挂查询。governor 持续压力会一路缩到此地板（设计意图），若地板 < 该最小尺寸则运行期挂查询。
- 准确公式（已核实）:`minCacheSize ≥ taskSlots × effectiveLoadQuantum`。
  - 并发数 = `taskSlots`（`SparkResourceUtil.getTaskSlots`，`GlutenPlugin.scala:144`）。
  - ⚠ `loadQuantum` 配置默认 **256MB**，但 doc（`VeloxConfig.scala:654-655`）明确「cache 启用时最多 8MB」→ 校验必须用**生效值** `min(配置loadQuantum, 8MB)`，**不能直接用 256MB 原值**（否则地板虚高，如 16×256MB=4GB）。
  - 参考:默认 16 slots × 8MB = 128MB < 512MB 默认 → 默认安全；风险在 taskSlots 大或 minCacheSize 调很小时（如 128 slots × 8MB = 1GB > 512MB → 危险）。
- **裁决:启动 fast-fail（与 #3 一致，非自动抬高）。** `start` 里校验 `minCacheSize >= taskSlots × effectiveLoadQuantum`，不满足则启动报错，逼用户改配置；不静默抬高。
- 待执行。

### 🟡 #5 —— `stop()` pinned 时 repay 0 泄漏 → **并入发现 4**
- 定位:`stop`（`CacheCapacityGovernor.scala:194-201`）。全 pinned 时 `adjustCapacity(-current)` 砍不动，`applied = current`，`released = 0` → `repay(0)`，借的（如 1GB）没还。注释（`:181`）"repays everything" 与实际不符。
- 同根:与发现 4 同病——**从 native `capacity()` 反推该还多少**，而非用 JVM 权威记录。
- **裁决:并入发现 4，不单独修。** 发现 4 新增 `var reserved` 权威字段后，`stop()` 改为无条件 `reservation.repay(reserved); reserved = 0`（stop 时 cache 整体销毁，pinned 与否无关，那些内存随 cache 一起没了，应无条件全额归还）。pinned 泄漏自然消失。
- 待执行（随发现 4）。

### 🟢 #6 CONFIRMED —— 见「发现 2」（dead code `getCacheUsedBytes`）。同一条。

### 🟢 #7 CONFIRMED —— 手写 daemon scheduler 重复轮子
- 定位:`CacheCapacityGovernor.scala:166-171` 手写 `Executors.newSingleThreadScheduledExecutor` + 自定义 ThreadFactory。
- 问题:本模块已用 `ThreadUtils.newDaemonSingleThreadScheduledExecutor`（如 `GlutenExecutorEndpoint`）；手写版漏了 Spark 工厂的 uncaught-exception handler → 线程致命错（非 NonFatal）静默消失。（维度⑥风格。）
- **裁决:改。** 换 `ThreadUtils.newDaemonSingleThreadScheduledExecutor("gluten-velox-cache-governor")`。
- 待执行。

### 🟢 #8 CONFIRMED —— `grow()` 丢弃 `adjustCapacity()` 返回值（多一次 JNI/tick）→ **并入发现 4**
- 定位:`grow`（`:124`）扔掉 `adjustCapacity(granted)` 返回值；下 tick `capacityBytes()`（getCacheCapacity JNI）重读。`shrink`（`:137`）正确用了返回值。
- 后果:每个 grow tick 2 次 JNI（adjust + 下 tick getCacheCapacity）而非 1。（维度②，非热路径锁，省 JNI。）
- **裁决:并入发现 4。** 发现 4 加 `reserved` 字段 + 每 tick 对账后，grow/shrink 记账逻辑重写，`adjustCapacity` 返回值本就必须用上（更新 reserved / capacity 基准），此冗余 JNI 随之消解。
- 待执行（随发现 4）。

---

## 维度专项

- **① 容量受限**:能受限，但 #1（驱逐用户块）+ #2（partial 卡死）令保证打折。
- **② 热路径锁**:未发现查询热路径锁问题（governor 在专用线程；`getCacheUsedBytes` 特意避开 `totalUsedBytes` 的 `numMallocBytes` 锁——但它 dead code）。#8 是 JNI 次数非锁。
- **③ 重复/dead/多余配置**:#6/发现2（dead code）、#7（重复 scheduler）、#8（冗余 JNI）、发现3（冗余 JNI 取配置）。
- **④ fast-fail/可 disable**:`pushback.enabled` 可关 ✅；#4 minCacheSize 无校验（缺 fast-fail）；#3 无 disable-on-error 降级。
- **⑤ DynamicOffHeapSizing 兼容**:`maxStorageMemory()`（`CacheCapacityGovernor.scala:223-234`）**已按 `dynamicOffHeapSizingEnabled` 选 on/off-heap storage**（`:227`，注释注明对齐 `GlobalOffHeapMemoryTarget`/`MemoryTargets.newConsumer`）。基础兼容已处理。**未验:dynamic 模式下查询走 `DynamicOffHeapSizingMemoryTarget` 的 `Runtime.maxMemory()` 门禁（不走 UMM），此时 governor 读 `maxOnHeapStorageMemory` 是否仍是有意义的信号——待补查。**
- **⑥ 风格**:#7 偏离 Spark thread 约定；C++ 侧未见 velox 风格问题。

## 已驳回（误报，不处理）
- JNI「delta 双解析」:shrink 期间 governed capacity 不变，`capacity()` 稳定，`targetFor` 两次一致 → 驳回。
- `cache==nullptr` 吞掉 shrink:allocator 与 cache 总是一起建 → 驳回。
- `maxStorage` 是「常量上限」:`maxOffHeapStorageMemory` 会减 execution 用量、确实跟踪 spare → 驳回。
- `VeloxBackend.cc` 移除 `dynamic_cast`:行为保持 → 驳回。

---

# 执行回复（2026-07-31，全部落地）

> 代码状态：Scala `PeriodicMemoryCheckerSuite` **20/20**，C++ `SparkMmapAllocatorTest` **13/13**，
> scalastyle / spotless / checkstyle / clang-format-15 全过。

## 逐条结果

| 项 | 裁决 | 结果 |
|---|---|---|
| 发现 1 | 改名 `PeriodicMemoryChecker` | ✅ class + object + 文件 + suite + 线程名 |
| 发现 2 | 删 `getCacheUsedBytes` | ✅ 仅删 JNI 层，`allocatedBytes()` 保留 |
| 发现 3 | 删 `getCacheStaticCapacity`，改读配置 | ✅ 连带去掉 `staticCapacity <= 0` 分支 |
| 发现 4 | `reserved` 权威 + 泄漏 | ✅ 见下，**修法比裁决更彻底** |
| #1 | 只改注释 | ✅ 不改行为 |
| #2 | 接受 all-or-nothing | ✅ 删死逻辑 + `FakeReservation` 对齐生产 |
| #3 | 驳回 review，保持 fail-fast | ✅ 维持现状，未改 |
| #4 | 启动 fast-fail | ✅ 公式简化，见下 |
| #5 | 并入发现 4 | ✅ `stop()` 无条件全额归还 |
| #6 | 同发现 2 | ✅ |
| #7 | 换 `ThreadUtils` | ❌ **撤回**，见下 |
| #8 | 并入发现 4 | ✅ 随接口重构消解 |

## 三处与 review 判断不同

### ① #7 做不到 —— `ThreadUtils` 是 `private[spark]`

review 的理由（"手写版漏了 uncaught-exception handler"）**不成立**：反编译
`ThreadUtils$.newDaemonSingleThreadScheduledExecutor` 确认它只做三件事 ——
`setDaemon(true)` + `setNameFormat` + `setRemoveOnCancelPolicy(true)`，**没有任何 exception
handler**。Spark 的 `SparkUncaughtExceptionHandler` 由 `Executor` 装在任务线程上，不走这个工厂。

更关键的是**根本用不了**：Spark 4.1.1 源码第 32 行 `private[spark] object ThreadUtils`。
`GlutenExecutorEndpoint` 能用是因为它在 `org.apache.spark.rpc` 包内；`PeriodicMemoryChecker`
在 `org.apache.gluten.memory`，编译期直接 `not found: value ThreadUtils`。

绕过去只能照 `SparkThreadPoolUtil.java:24` 的先例新增一个 `org.apache.spark.util` 下的 Java
转发类（Java 不认 Scala 的 `private[spark]`）。为了替换 6 行、且唯一实质差异
（`removeOnCancelPolicy`）我们用不上（只 schedule 一个永久任务，从不 cancel 单个任务），
不划算。代码里留注释说明。

> **顺带指出 review 未覆盖的真实风险**：`scheduleWithFixedDelay` 在任务抛异常时会
> **永久取消整个调度且无日志**。现靠 `onTick` 的 `NonFatal` catch 挡住，但 `NonFatal`
> 不接 `OutOfMemoryError` 这类 —— 真出了 governor 会悄无声息停摆，外部无迹象。
> 两个版本都不防这个，与 `ThreadUtils` 无关。**未处理，留作后续。**

### ② #4 的 `min(loadQuantum, 8MB)` 是多余的

review 特别警示"必须用生效值，否则地板虚高（16×256MB=4GB）"。该场景**不可达**：
`VeloxListenerApi.scala:88-93` 在 `onDriverStart` 强制 —— 开 cache 且 `loadQuantum > 8MB`
直接抛 `IllegalArgumentException`，driver 起不来。故能走到 governor 的路径上
（`:243` 已 gate cache enabled）必然 ≤ 8MB，直接用配置值即可。

（设计文档 §2.3 原写"doc 建议、代码无强制"，同为旧说法，已一并更正。）

### ③ 发现 4 的修法从"对账"升级为"改接口"

review 建议每 tick 开头对账（`if (reserved > actual) repay(diff)`）。这一版**先按此实现并验证**
（删掉修复代码重跑，回归测试立刻失败，差 156MB），随后用户指出：既然 `reserved` 是唯一信源，
那 `getCacheCapacity` 就不该存在 —— 它唯一的用处是把算好的目标翻译成 delta。

⇒ **C++ 接口 `adjustCapacity(delta)` → `setCapacity(from, to)`**（两个绝对值）。连带删除
`getCacheCapacity` JNI、`targetFor()`、以及刚加的 `reconcile()`。**JNI 面现在只剩一个函数。**

## 超出 review 范围、额外修的 4 处

1. **`getCacheCapacity` 返回 `governedCapacity()` 是独立 bug**（review 记为"注"并判断
   "根因不在读哪个值"）。native 的 `targetFor`、测试的 `FakeNative` 都用 clamp 后的
   `capacity()` —— **三方基准不一致**。加上对账后会变危险：pin 时读到 governed=200MB
   而实际占 380MB，会把 180MB 还给 Spark 而 cache 还在用 ⇒ `reserved < capacity`，
   正是会 OOM 的方向。已随接口重构消失。

2. **不变式 I3 移进 native 且无条件强制**：
   ```cpp
   VELOX_CHECK_GE(from, capacity(), "Cache is holding more memory than has been reserved for it");
   ```
   原 I3 表述为"涨必精确"且只在 `delta > 0` 时查。用户指出更强的性质是
   **`reserved >= capacity()` 任何时刻成立**，"涨必精确"只是它的推论
   （`to > from >= capacity() >= used` ⇒ `max(to, used) == to`）。方向判断因此从逻辑里
   彻底消失。**不变式跟数据结构放在一起，新增调用方绕不过去**——这正是 `from` 参数的理由。

3. **v3 死代码三处**（squash 时才暴露，被中间 commit 掩盖）：
   - `GlutenPlugin.scala` −14 行：关于 `spark.memory.unmanagedMemoryPollingInterval` 的警告。
     **有害** —— v5 用自己的调度器 + `MemoryTargets.global()`，根本不碰 Spark 的 unmanaged
     轮询器；留着会告诉运维"不设这个参数就没有真正的保护"，而设了也没用。
   - `SparkShims.scala` −1：删 `registerVeloxCacheConsumer` 后的空行。
   - `Spark41Shims.scala`：`with Logging` + import，全文件无一处 `log*` 调用。

   清理后三个文件与 base **逐字节一致**，反证其为纯残留。

4. **测试基础设施缺陷**：原来所有测试都绕过 `start()`，`reserved` 恒为 0，**记账逻辑完全没测到**。
   拆出 `reserveInitial`，测试现从"已向 Spark 登记过当前容量"的真实状态出发。另有 3 个测试直接调
   `native.adjustCapacity` 背着 checker 改容量 —— 那不是淘汰，已改为通过 checker 设置起始容量。
   `FakeNative` 现在也强制同一条不变式，两侧模型一致。

## 维度专项的更新

- **① 容量受限**：#2 的"partial 卡死"担忧解除 —— Spark 本就只有全额或 0，慢增小步长兜底即可。
- **③ 重复/dead**：JNI 从 4 个函数降到 **1 个**。
- **④ fast-fail**：#4 已补（§3.7.1）。
- **⑤ DynamicOffHeapSizing**：review 提出的待查项**仍未验证** —— dynamic 模式下查询走
  `DynamicOffHeapSizingMemoryTarget` 的 `Runtime.maxMemory()` 门禁（不走 UMM），
  此时读 `maxOnHeapStorageMemory` 是否仍是有意义的信号。**保留为未决项。**

## 仍未做

- 集群标定 `minCacheSize` / `cacheRatio`（需实测并发读线程数）
- TPC-DS 开/关 pushback 端到端对比
- 跨 profile 编译（`-Pspark-3.3` / `-Pspark-3.5`；注意 3.5 有**既有的** `gluten-ui` 失败，
  与本改动无关，已通过 stash 验证）
- 维度⑤的 dynamic-off-heap 信号有效性
- `scheduleWithFixedDelay` 遇 fatal error 静默停摆（见 ① 的补充）

---

# 第二轮 Review（`280d284d..82447e10`，含 reserved 落地 `c80811660`）

> 上一轮裁决多已落地:`c80811660` 把 `adjustCapacity(delta)` 重构为 `setCapacity(from, to)`（绝对值语义 + reserved 权威记录，对应上一轮发现 4）。以下为本轮新增关注。后台 code-review 结论待并入。

## 第 7 点（用户新增）—— `SparkMmapAllocator::setCapacity` 的 `to` 未 page 对齐
- 定位:`SparkMmapAllocator.cc:36-53` `setCapacity(from, to)`——`governedCapacityBytes_.store(to)`，`to` 是 JVM 传来的任意字节（reserved/`maxStorage×ratio`/minCacheSize 等），**未对齐 page**。
- velox 惯例:内存按 page 管理（`AllocationTraits::kPageSize=4096`）。`allocatedBytes()=pageBytes(numAllocated())` 恒 page 对齐；`capacity()=max(governedCapacity(), allocatedBytes())` 里 governed 非对齐 → 两口径混用。
- 潜在后果（方向，待 velox 侧确认）:(1) `capacity()-allocatedBytes()` 的 headroom 非 page 整数倍，`makeSpace` 边界判断不确定；(2) `VELOX_CHECK_GE(from, capacity())` 两侧对齐口径不一致，边界误触发；(3) 违反 velox page 对齐惯例（维度⑥ C++ 风格）。
- ⚠ 改法取舍（关键）:velox 只提供 **ceil** helper（`AllocationTraits::roundUpPageBytes` / `numPages` 用 `bits::roundUp`），无现成 floor。
  - **ceil**:对齐 velox 惯例，但 `governedCapacity` 可能 > reserved（`from`）→ 破坏「容量 ≤ 预留」不变式，`VELOX_CHECK_GE(from, capacity())` 更易误触发。
  - **floor**（`to / kPageSize * kPageSize`，需自写）:守「容量 ≤ 预留」不变式，但偏离 velox helper。
  - 倾向 **floor**:容量宁少算（不超预留）不多算（声称有但装不下）。
- **裁决:待定（需用户定 ceil/floor + 是否在 setCapacity 内对齐）。**

## 本轮待处理清单（4 条）

### 🔴 新#1 —— benchmark `adjustCapacity` 编译错误
- 定位:`cpp/velox/benchmarks/SparkMmapAllocatorBenchmark.cc:84`（review 报 :90，实际调用在 :84）:`allocator->adjustCapacity(-static_cast<int64_t>(kCapacityBytes - kLoadQuantum*4))`。
- 问题:`c80811660` 已把 `adjustCapacity(delta)` 重构为 `setCapacity(from, to)`，`adjustCapacity` 不复存在。benchmark 是注册 target（`add_velox_benchmark spark_mmap_allocator_benchmark`），开 benchmark 即编译失败「no member named adjustCapacity」。重构漏改调用点。
- 改法:改 `setCapacity(kCapacityBytes, kLoadQuantum*4)`（绝对值语义）。
- **裁决:待定（必修，真编译错）。**

### 🔴 额外#1 —— `checkFloorFitsConcurrentReads` 校验默认配置下必然失败（我复核新增）
- 定位:`PeriodicMemoryChecker.scala:325-337`。`needed = taskSlots × conf.get(LOAD_QUANTUM)`；`minCache < needed → 抛 IllegalArgumentException`。
- 关键核实:Gluten **未在代码里把 loadQuantum clamp 到 8MB**——`ConfigExtractor.cc:336` 直接把配置值（默认 **256MB**）传 velox；「cache 开时最多 8MB」只是 `VeloxConfig.scala` doc 的文字建议，非强制。
- 后果:默认配置下 `needed = taskSlots × 256MB`（如 16 slots → 4GB）。minCacheSize 默认 512MB < 4GB → **开 pushback + 默认配置即启动抛异常**，executor 起不来。**这个 fast-fail 默认就误杀正常配置，过于激进。**
- 注:上一轮记录里「用生效值 min(配置,8MB)」的建议也不成立（代码无此 clamp）。真问题是校验与默认配置不兼容。
- 改法（三选一，需裁决）:(a) 校验里对 loadQuantum 取 `min(配置, 8MB)`（体现 cache 场景建议值）；(b) 校验改 warn 不 throw（别误杀）；(c) 调默认值使默认组合自洽。
- **裁决:待定。**

### 🟡 新#2 / 第7点 —— `capacity()` 未 page 对齐，cache 可超预留 < 1 page
- 定位:`SparkMmapAllocator.cc:32` `capacity() = max(governedCapacity(), allocatedBytes())`；`setCapacity` store 原始 jlong `to`。
- 机制（review 确认）:`AsyncDataCache::canTryAllocate` 把 `capacity()` 喂 `AllocationTraits::numPages()`（`bits::roundUp(bytes, 4096)`，**向上取整**）→ 非对齐 governed cap 使 page 预算多算最多 4095 字节 → cache 超预留 < 1 page。velox 基类按 page 存 capacity 避开此问题。
- velox helper:只有 ceil（`roundUpPageBytes`/`numPages`），无 floor。
- 取舍:**floor**（`to/kPageSize*kPageSize`，自写）守「容量 ≤ 预留」不变式（倾向）；ceil 对齐 velox 惯例但可能破坏不变式。
- 改法:`capacity()` 里对 `governedCapacity()` floor 到 page 后再 max。
- **裁决:待定（ceil/floor）。**

### 🟡 额外#2 —— #7 ThreadUtils 无法直接换（我复核新增）
- 定位:`PeriodicMemoryChecker.scala:181-183`。手写 `Executors.newSingleThreadScheduledExecutor`。
- 核实:代码注释已说明——`ThreadUtils.newDaemonSingleThreadScheduledExecutor` 是 `private[spark]`，`org.apache.gluten.memory` 包访问不到，故保留手写。**上一轮裁决「换 ThreadUtils」无法直接执行。** 但手写版仍漏 uncaught-exception handler（原 #7 的实质关切）。
- 改法（需裁决）:(a) 手写版补 `setUncaughtExceptionHandler`；(b) 把类挪到 `org.apache.spark.*` 包以用 ThreadUtils；(c) 接受现状（onTick 已有 NonFatal，致命错另说）。
- **裁决:待定。**

---

## 第二轮执行回复（全部落地，C++ 15/15、Scala 23/23）

### 第 7 点 / 新#2 —— page 对齐:**裁决 floor，已实现**

采纳「容量宁少算不多算」。两处一并对齐:

1. `setCapacity` 存入前 `pageAlignedDown(to)`（`SparkMmapAllocator.h` 私有静态方法，velox 无 floor helper 故自写）
2. 构造函数 `governedCapacityBytes_(pageAlignedDown(options.capacity))`

选 floor 而非在 `capacity()` 里对齐:store 时对齐一次，比每次读都算一遍更省——`capacity()` 在热路径上（每次 `canTryAllocate`）。

**floor 的额外好处**:JVM 传任意字节数都安全，无需知道 page 大小。C++ 只会把上限**收紧**，`allocated ≤ governed ≤ reserved` 自动成立，两侧不需要协调对齐口径。

回归测试:`doesNotAdmitBeyondAnUnalignedCapacity`（C++）——设 `4MiB+1`，断言 `allocatedBytes() <= reserved` 且其后 `setCapacity` 不抛。Scala 侧 `FakeNative` 同步镜像 floor，立刻抓出一处断言过强（原断言容量恰等于目标值）。

### 新#1 —— benchmark 编译错误:**已修**

改为 `setCapacity(allocator->staticCapacity(), kLoadQuantum * 4)`。用 `staticCapacity()` 而非 `kCapacityBytes` 当 `from`:构造后 `governed == staticCapacity()` 且 `allocated == 0`，故 `from == capacity()` 恒成立，**不受 velox 取整影响**。

### 额外#1 —— `checkFloorFitsConcurrentReads` 默认必失败:**误报，无需修改**

核实方向对（`ConfigExtractor.cc:336` 确是透传），但强制不在传参处，而在 **driver 启动时**:

```scala
// VeloxListenerApi.scala:88-93，onDriverStart 内
if (conf.get(COLUMNAR_VELOX_CACHE_ENABLED) && conf.get(LOAD_QUANTUM) > 8 * 1024 * 1024) {
  throw new IllegalArgumentException("Velox currently only support up to 8MB load quantum ...")
}
```

链条闭合:pushback 开 ⟹ 校验要求 cache 也开（否则告警跳过）⟹ driver 已验过 `loadQuantum ≤ 8MB` ⟹ `needed ≤ 16 × 8MB = 128MB < 512MB` 默认底线。**默认配置安全。**

故上一轮「用 `min(配置, 8MB)`」的建议确实多余——不是因为 clamp 存在，而是因为大于 8MB 的配置根本活不到 executor。

### 额外#2 —— ThreadUtils:**接受现状（选项 c）**

反编译 `ThreadUtils$.newDaemonSingleThreadScheduledExecutor` 确认它只做三件事:`setDaemon(true)` + `setNameFormat` + `setRemoveOnCancelPolicy(true)`，**没有 uncaught-exception handler**。Spark 的 `SparkUncaughtExceptionHandler` 由 `Executor` 装在任务线程上，不走这个工厂。故选项 (a)「补 handler」补的是一个 `ThreadUtils` 本来也没有的东西。

真正的风险在别处:`scheduleWithFixedDelay` 在任务抛异常时**永久取消整个调度且无日志**。现靠 `onTick` 的 `NonFatal` 挡住，接不住 `OutOfMemoryError` 一类。**未处理，留作后续**——两个版本都不防，与 ThreadUtils 无关。

### 本轮另修两处（不在 review 清单内）

**④ 拒借时 cache 容量被设为 0**（`reserveInitial`）:`borrow` 全额或 0，返 0 时直接 `setCapacity(max, 0)`，违反 I2，查询撞 `kNoCacheSpace`;且 grow 分支只有上界，从 0 爬不回底线。

现在按「启动时就是 `maxCacheBytes`」简化——allocator 从建出来就占着配置的量，如实登记，借不到即配置错误抛异常。**clamp、退让借款、`storageFraction × cacheStorageRatio` 初始值推算全部删除**。grow 分支补下界 `clamp`。

**② `staticCapacityBytes_` 口径不一致**:原取 `MmapAllocator::capacity()`（velox 向上取整到 64MiB 倍数:`largestSizeClass=256` × 64 页），而 JVM 用配置原值 ⟹ **任何非 64MiB 倍数的 memCacheSize 都会在 executor 启动时抛**（100MB → capacity()=128MiB > from=100MB）。

单测用 64MB（1 个量子）、默认配置 1GB（16 个量子）**都碰巧对齐**，故从未暴露。

现改用 `options.capacity`。两边构造时即一致，`reserveInitial` 因此不再需要调 JNI（原有的 `setCapacity(initial, initial)` 已删）。velox 物理持有量 ≥ 我们的上限，只会更保守。

回归测试:`startsFromTheConfiguredSizeNotTheRoundedOne`（C++，配 100MiB）。

### ⑤ dynamic off-heap 闸门:**已修**

`CacheStorageReservation` 改用 `MemoryTargets.dynamicOffHeapSizingIfEnabled(MemoryTargets.global())`。非 dynamic 模式下该包装原样返回，行为不变。

回归测试:`CacheStorageReservationDynamicSizingSuite`（gluten-core）。放在 `org.apache.gluten.memory.memtarget` 包下才能读到包内可见的 `usedOffHeapBytesForTesting()`——那个静态计数器正是闸门的判据，从外部无法观测。

测试可行的关键:`GlobalOffHeapMemoryTarget.borrow` 在无 SparkEnv 时走 `.getOrElse(size)` 直接返回，故不需要真实的 Spark 内存管理器。

**验证**:回退成裸 `global()` 后立刻失败（`0 did not equal 4096`）。

### ⑥ 配置校验:**已修**

6 个配置补 `checkValue`。两个比例限定开区间 `(0, 1)`——`shrinkRatio >= 1` 会让"紧张时"算出比当前更大的目标，转而去借更多内存，**算法整个反转**。其余限定 `> 0`。

### `scheduleWithFixedDelay` 遇 fatal error 静默停摆:**明确不做**（用户裁决）

已知行为:`scheduleWithFixedDelay` 在任务抛异常时**永久取消整个调度且无日志**。`onTick` 的 `NonFatal` 挡得住普通异常，接不住 `OutOfMemoryError` 一类。真发生时 governor 悄无声息停摆，cache 容量冻结在当时的值。

不做的理由:能抛 fatal error 的 executor 本身已经处于严重故障，governor 停摆不是主要矛盾;而要防住它就得加看门狗或重新挂载调度，为一个 JVM 大概率正在死亡的场景增加常驻复杂度，不划算。

冻结的后果是有界的:容量停在最后一次设定值，该值已向 Spark 登记过，故不会出现"用了没登记的内存"。只是不再随压力调整。

与 ThreadUtils 无关——两个版本都不防（见额外#2）。

---

# 第三轮 Review（`8767ffe8b..HEAD` 全量，三个 agent 并行）

分工:`rev-correctness`（GPT-5.6 Sol，不变式/时序/算术/AIMD 极端/生命周期）、`rev-spark`（Claude Opus 4.6，Spark 内存配置矩阵/MemoryTargets 组合/压力信号/block 淘汰/executor 生命周期）、`rev-cpp`（Gemini 3.1 Pro，MmapAllocator 子类化/velox 风格/JNI 边界/对象生命周期）。

## 已修 3 条

### 🔴 `stop()` 过度归还（`rev-correctness` #2）

前提已核实:`VeloxListenerApi.scala:265` 的 `// TODO shutdown implementation in velox to release resources` 说明**此时 backend 并未销毁**。故上一轮写的「cache 随 backend 整体销毁，pin 与否无关」**是错的**——那正是上一轮「修好」的东西。

pin 住时原代码无条件归还全部 `reserved`，而 cache 仍占着内存;Spark 关闭任务池时**不等待运行中的任务**，那些任务可能把这块"已释放"的内存再拿一次。

改为与普通轮次同规则:只还 `reserved − applied`;异常时**一分不还**（宁可少还）。留在账上的随 executor 消失。回归测试:`does not repay memory the cache is still holding on stop`。

### 🔴 底线校验与页对齐口径不一致（`rev-correctness` #3）

校验用原始字节，而容量存入时向下对齐、分配按 `numPages` 向上取整 ⟹ 校验会放过一个「每读者短最多一页」的底线。两侧改为按整页:`floor(minCache) >= taskSlots × ceil(loadQuantum)`。

### 🟡 死区大于整个调节范围（`rev-correctness` #4）

`deadZone >= maxCache − minCache` 时任何调整都过不了过滤，**特性看着开着实则完全不工作**。加启动校验 `checkDeadZoneLeavesRoomToMove`。

（同条目里的 `growRatio = Infinity` 溢出未单独处理——`checkValue(_ > 0)` 确实放行 `Infinity`，但那已属于蓄意的非法配置，且 §5 的 `(0,1)` 约束覆盖了 `shrinkRatio`/`cacheRatio` 这两个会反转算法的。）

## 一条驳回

### `rev-cpp` 报的 CRITICAL「析构顺序导致 UAF」——**判断有误**

它称 `VeloxBackend` 先析构 `cacheAllocator_`，`~AsyncDataCache` 随后经裸指针 `freeData()` 触发 UAF，建议调换声明顺序。

**照做后被测试证伪**:`cache.reset()` 之后 `allocatedBytes()` 仍是 65536。根因是 **allocator 持有 cache 的 `shared_ptr`**（`MmapAllocator.h:397`，`registerCache` 所设）——**循环引用**，cache 压根不会析构，UAF 到不了。

调换声明顺序**解决不了任何问题**，已撤回。真相见设计文档 §3.9:`AsyncDataCache::shutdown()` 是释放页的唯一途径，而它由 `NativeBackendInitializer:62-67` 注册的 shutdown hook 保证。回归测试 `releasesPagesOnlyOnCacheShutdown` 把这条契约固化下来。

**附带纠正**:声明顺序是既有的（`git show 8767ffe8b:` 可验），本次只改了类型。

## 一条不同意

### `rev-correctness` #1「容量不是硬上限」

事实描述正确——并发竞争确实能让 `allocated` 短暂越过 cap（详见设计文档 §8.1，含算例与上界）。

但它给的修法是「Spark 必须预留 allocator 取整后的完整容量，容量治理不能安全归还内存」——**那等于废掉整个特性**。它把「不是硬上限」当成了「没有价值」:实际是软上限，超出量有界（`(并发数−1) × loadQuantum`，16 slots 约 120MB）且自收敛（余量转 0 后分配全挡）。用有界的瞬时超发换 GB 级内存的可回收性，取舍成立。

根治需要在 velox 内做原子准入 —— 越出「只改 Gluten」的约束。

## 全部通过的部分

`rev-spark`:**5 个领域全通过，无高危**。含堆外未开启时的 fail-fast、幽灵预留淘汰不掉且池不变式成立、`stop()` 在 `shutdown()` 中排位正确、dynamic 模式 mode 选择与预留落点一致。唯一低危观察（`untracked=true` + `pushback` 信号失真）已记入设计文档 §8.4，未加告警。

`rev-cpp`:除那条驳回外 4 个领域通过。含 `capacity()` 覆写安全（基类内部用 `capacity_` 成员）、`toString()` 会走虚函数且不会下溢、`memory_order_relaxed` 恰当、JNI 类型转换有 `VELOX_CHECK_GE` 保护、`shrink()` 的 `> 0` 契约被 `if (used > want)` 保证。

`rev-correctness` 明确验证通过:普通 grow/shrink/reserveInitial 的异常时序偏向多预留、I4 正确、JNI 转换在 64 位安全、`onTick`/`stop` 锁无死锁、dynamic 包装与查询同闸门。

---

# 第四轮：把「容量不是硬上限」查到底

第三轮驳回了 `rev-correctness` #1 的**修法**，但它指出的**事实**一直没有量到底。本轮把它做完了，
结论是:事实成立，但根治只能在 velox 内做 —— 三条不改 velox 的路线全部试过并有明确的失败证据。

完整证据与给上游的提案见
[`specs/2026-08-01-velox-admission-capacity-handoff.md`](./specs/2026-08-01-velox-admission-capacity-handoff.md)
（该文档已经过第五、六轮两次重写，本轮的结论「根治只能在 velox 内做」成立，但当时提的
具体方案在后两轮都被否决了）。

## 真正的根因（第三轮没说准）

不是「无锁检查」这么笼统，而是**报告与执行分家**:

- `MmapAllocator::capacity()` 是虚函数，我们覆写它把上界降下来 —— 这只改了
  `AsyncDataCache::canTryAllocate()` 读的**建议值**
- velox 真正发页的地方（`MmapAllocator.cpp:90` / `:301` / `:409`）打的是构造时定死的
  `const capacity_`，是一次原子 `fetch_add` + 检查 + 回滚，**本身完全精确**

**velox 没有错**:原版 `capacity()` 返回的就是 `pageBytes(capacity_)`，两者永远一致。
这道缝是「覆写 `capacity()`」这个用法引入的，是我们的。

## 量到的数

16 线程 × 3000 × 8KB entry、授予 32MB（`SparkMmapAllocatorTest::holdsTheGrantWhileReadersFillTheCacheTogether`）:

| | 静止后真实页 |
|---|---|
| 现方案（覆写 `capacity()`） | ≤ 授予量，**偶尔超 8KB**（一个 entry，0.02%） |
| 加上可调容量后 | ≤ 授予量，**精确** |

理论上界 `(并发数−1) × loadQuantum` ≈ 120MB 是对的，但**实测远小于此**:
cache 满了以后根本不走那个无锁检查（检查失败 → 淘汰 → 腾出的页以 `AcquiredMemory`
直接回到本线程 → 重试按 `requestBytes <= acquiredBytes` 满足，不查余量）。先释放再分配是精确的。

**过程中的一次误判（记下来）**:头几轮全过，我据此说「就是没问题」并提议回滚修复。
多跑几轮才复现。**「跑了几次没复现」不等于「没有」**——尤其对并发缺陷。

## 三条不改 velox 的路线，全部失败

| 路线 | 结果 |
|---|---|
| 覆写 `allocateNonContiguousWithoutRetry` | `MmapAllocator` 在自己的 **private** 段重新声明了该 override（`:133`→`:300`），**能覆写不能委托**；重写要碰 9 个私有成员 |
| 包装 `Cache::makeSpace` + 自做原子占位 | 真实页精确不超，但占位记在 `numAllocated_` 上，别的线程读到的 `allocatedBytes()` 虚高最多 `线程数 × 请求`（实测 24KB） |
| 把 `MmapAllocator` 整类拷进 Gluten（1392 行） | **编译期挡死** |

最后一条是决定性的。`Allocation.h:193-195`:

```cpp
 private:
  void append(uint8_t*, MachinePageCount);
  void clear() { ... }

  friend class MemoryAllocator;
  friend class MmapAllocator;
  friend class MallocAllocator;
```

写入 `Allocation` 的权限只发给三个指名道姓的类。`gluten::SparkMmapAllocator` 派生自
`MemoryAllocator`，但 **C++ 友元不继承**，基类也没留 protected 转发口。编译器给了 6 个
`is private within this context`。

要拷贝就必须往 velox 里加 `friend class ::gluten::SparkMmapAllocator;` ——
**在 velox 里写死 Gluten 的类名，永远上不了游**。而这比直接加 6 行
`dynamicCapacity_` 更差。

**所以「拷贝到 Gluten 就能绕开改 velox」是错的。** 拷贝也要改 velox，且改得更差。

## 本轮改动

代码**净变化只有测试**:

- 新增 `SparkMmapAllocatorTest::holdsTheGrantWhileReadersFillTheCacheTogether` ——
  并发填充，容差就是那个洞的实测大小，注释写明「这是测量不是保证」并指回 handoff
- 新增 `PeriodicMemoryCheckerSuite::cannot yield while the cache is over its reservation`
  —— Scala 侧证明后果:一旦 `used > reserved` 且有 pin，10 个 tick 什么都做不了，
  **升降都被挡住**（不只是不能升），且 `onTick` 把异常吞掉，静默
- 设计文档 §8.1 用实测数替换了理论上界，并新增 §8.2「降容不立刻退还 RSS」（**第五轮已推翻并删除**）

验证用的 fork（1600 行）**已全部回滚**，它证明过的东西记在 handoff 里。

## 顺带发现（已写入 handoff §3.3）

降容量**不会立刻退还 RSS**。释放 cache entry 后页回到 size class 空闲表但**仍是 mapped**，
而 `ensureEnoughMappedPages` 只在 `numMapped_` 超过静态 `capacity_` 时才 `adviseAway` ——
我们降的是报告值，碰不到它。所以上游那个改动里，**映射阈值也必须跟随可调容量**，
且 `setCapacity` 降容时应主动调一次已有的 `unmap()`，否则"把内存还给 Spark"在 RSS 层面是假的。

---

# 第五轮：handoff 被外部评审推翻，方案改到 cache 上

第四轮产出的 handoff 提案是「给 `MemoryAllocator` 加 `setCapacity()`」。外部评审提了六条，
**核对源码后五条成立**（第 1 条未拿到），其中一条推翻了我在**同一天**刚写进文档的东西。
handoff 已按新方案重写。

## 五条成立的意见

| # | 事实 | 证据 |
|---|---|---|
| 2 | 会打破 `MemoryManager` / arbitrator 不变式 | `Memory.cpp:167` 只在**构造时**校验 `allocator_->capacity() >= arbitrator_->capacity()`；`Memory.cpp:252-254` `MemoryManager::capacity()` 直接返回 `allocator_->capacity()`。改 allocator 容量 ⇒ MemoryManager 对外报告跟着变、arbitrator 毫不知情 |
| 3 | §3.5 的 clamp 方案不成立 | `numAllocated()` 在 clamp 之后随时会涨，`AsyncDataCache.cpp:1046-1049` 与 `CachedBufferedInput.cpp:100-101` 的无符号减法照样回绕。真正的修法是调用点改饱和减法 |
| 4 | `MallocAllocator` 的 `capacity_ == 0` 表示无限 | `MallocAllocator.h:186-187` 注释原话。「never above construction capacity」遇上 0 直接失效 |
| 5 | 加纯虚会打断树外实现 | `MemoryAllocator.h:212-213`「Proxy subclasses may provide context specific tracking while delegating the allocation to a root allocator」—— 公开扩展点 |
| 6 | `unmap` 不是新能力 | `AsyncDataCache.cpp:1027` `shrink()` 里本来就有 `allocator_->unmap(...)` |

## 第 6 条推翻了我自己（同一天写的）

第四轮我"顺带发现"并写进设计文档 §8.2 和 handoff §3.3 的结论 ——
**「降容不退还 RSS，必须改 `ensureEnoughMappedPages`」—— 是错的**。

- `AsyncDataCache::shrink()` 本来就调 `allocator_->unmap()`
- 而 **Gluten 自己的 JNI（`VeloxJniWrapper.cc:1327`）降容前就在调 `cache->shrink(used - want)`**

也就是说：这条路径上的 `unmap` 一直都在跑，我在 fork 实验里加进 `setCapacity` 的那个
`unmap` 基本是冗余的。**我是从 fork 实验的观察直接下的结论，没有 grep 既有代码。**
设计文档 §8.2 已删除，条目重新编号。

这和第四轮记的那次误判是同一类错误：**观察到现象就下结论，没有先查现有实现。**

## 一条实质异议（已核对，成立）

评审的替代方案是「把上界放在 cache 的 `cachedPages_` 上，两个分配器一行不改」——
方向正确，但其中一句「`canTryAllocate` 可选地把 cache 上界算进去…它不是正确性依赖」**不成立**。

淘汰重试的机器在 `MemoryAllocator::allocateNonContiguous`（`MemoryAllocator.cpp:224-233`）:

```cpp
success = cache()->makeSpace(pagesToAcquire(...), [&](AcquiredMemory& acquired) {
  acquired.free(this);
  return allocateNonContiguousWithoutRetry(mix, out);   // ← 失败才触发淘汰
});
```

`AsyncDataCacheEntry::initialize()` 是**调进这个环里**的。上界若只在 `initialize()` 里、
在调分配器**之前**拦截，失败到不了 `makeSpace`，**淘汰不会被触发**，直接
`VELOX_CACHE_ERROR` → `kNoCacheSpace`。而 `kNoCacheSpace` 全仓库只有
`exec/fuzzer/CacheFuzzer.cpp:497` 一处 catch，**生产路径无人接**。

后果:「cache 到达上界」这个**稳态**会让之后每一次 miss 都抛未捕获的致命错误。
所以 `canTryAllocate` 认上界是**活性依赖**，不是优化。handoff §2.3 已按此写。

## 新方案为什么更好

| | allocator 上界（已否决） | cache 上界（现方案） |
|---|---|---|
| `MemoryManager` / arbitrator | **静默破坏不变式** | 零改动 |
| 报告与执行 | 分家，缝里能漏（实测 8KB） | `cachedPages_` 原子占位，执行即报告 |
| `allocatedBytes()` | 被占位污染（实测虚高 24KB） | 不受影响 |
| `MmapAllocator` / `MallocAllocator` | 都要改 | 都不改 |
| 默认行为 | 需要论证等价 | 默认无界 ⇒ 逐字节不变 |
| `Allocation` 的 friend 死结 | 撞上 | 不涉及（`cachedPages_` 是 cache 自己的） |

## 状态

- handoff 已重写：cache 上界为提案，allocator 上界移入「已否决」并附五条理由
- 设计文档 §8.2 删除、条目重新编号
- **评审的第 1 条未拿到**，尚未评估
- 代码零改动（Gluten 现状本来就是接受 §3.2 那个洞的薄子类）

---

# 第六轮：上游已有可用的扩展点，方案落地

第五轮把方案改到 cache 上界，是因为「allocator 上界会打破 `MemoryManager` 不变式」。
本轮发现**上游已经有了正确的做法**，两条自拟方案都不需要了。

## 上游的做法：`admissionCapacity` 参数化

`MmapAllocator` 把三条准入路径的容量提成**按值传入的 protected 方法**：

```cpp
protected:
  bool allocateNonContiguousWithCapacity(const SizeMix&, Allocation&, MachinePageCount admissionCapacity);
  bool allocateContiguousWithCapacity(..., MachinePageCount admissionCapacity);
  bool growContiguousWithCapacity(..., MachinePageCount admissionCapacity);
```

原入口变成一行转调传 `capacity_` —— **纯重构，行为逐字节不变**。

它一次性避开了第五轮列的全部四条：

| 第五轮的意见 | 在这个设计里 |
|---|---|
| #2 `MemoryManager`/arbitrator 不变式 | **不涉及** —— `capacity_` 与 `capacity()` 一字未动 |
| #3 无符号减法回绕 | **不涉及** —— `canTryAllocate` 读的还是原来的 `capacity()` |
| #4 `MallocAllocator` 的 `capacity_ == 0` | **不涉及** —— 只动 `MmapAllocator` |
| #5 纯虚打断树外实现 | **不涉及** —— 没加任何虚函数 |

而且 `ensureEnoughMappedPages(newMappedNeeded, admissionCapacity)` **按值传参**，
我第五轮担心的「两次读之间容量升高导致 `uint64_t` 下溢」从设计上就不存在。

## Gluten 侧只需 40 行

覆写三个私有虚函数（可覆写、不可外部调用），转调 protected 版本传 `governedPages()`。
**分配体留在基类，完全不碰 `Allocation`** —— 第四轮那个 friend 名单的编译期死结不存在。

## 实测

| | 峰值超出 |
|---|---|
| `cachedPages()` 真实缓存页 | **0**，三次全部 |
| `numAllocated_` 计数器 | +8KB |

那 8KB 是 `fetch_add` 与 `fetch_sub` 之间被别的线程读到的**正在被拒绝的请求**，不是内存。
用 `cachedPages()`（只在分配成功后才更新）做对照测出来的。

> **过程中的一次误判**：这一条我先凭推理判断"计数器虚高不是真实超发"，
> 但第四轮我刚因为"凭推理下结论"栽过，所以这次写了探针实测。**结论对了，方法这次才对。**

`BM_CacheAllocate`：base 348ns / governed 348ns，**无差异**。

## 顺带否决了「加读写锁」

用户问加读写锁关死降容窗口的代价，实测：

| 线程数 | 裸原子 | `folly::SharedMutex` | `std::shared_mutex` |
|---|---|---|---|
| 1 | 0.22 ns | 9.8 ns | 11.6 ns |
| 16 | 0.24 ns | 11.1 ns | **1482 ns** |

`std::shared_mutex` 的读锁内部是同一 cache line 上的原子 RMW，读者互相打架 ——
16 线程下单是锁就比整个分配（348ns）贵 4 倍，排除。folly 版约 +3.2%，
但换来的只是「少延迟一轮记账」，不划算。

## 一个 assert 堵死了唯一的出路（用户发现）

```cpp
VELOX_CHECK_GE(from, capacity(), "Cache is holding more memory than has been reserved for it");
```

撞上降容窗口后它抛，`onTick` 接住，**容量根本没降下去** ——
而降容量正是逼 cache 淘汰、把内存吐出来的唯一手段。于是每一轮都抛，永远出不来。

`shrink()` 里那个 `applied > reserved` 分支早就写好了，只是被这个 assert 挡着走不到。

改成如实上报，JVM 侧在 `grow` 和 `shrink` 里**先借后记**补账。
Scala 测试从 `cannot yield while the cache is over its reservation`（记录卡死）
换成 `settles up...` + `keeps shrinking once the pinned entries are released`
（**卡死变自愈**）。

## 自查中发现的漏账

用户问「现在 OK 了吗」，复查时发现 `grow()` **丢弃了 `setCapacity` 的返回值**：

```scala
reserved = target
setCapacity(from, target)   // ← 返回值直接丢了
```

cache 已超出 target 时返回的是实际占用，但 `reserved` 被写死成 target，**差额永远不记账**。
`shrink` 上一步补了，`grow` 漏了。已修，并补了回归测试。

顺带发现原来那个 grow 测试测的就是这个场景，只是断言写的是漏洞行为，两个测试已合并。

## 验证

| | 结果 |
|---|---|
| `spark_mmap_allocator_test` | **17/17**，连跑 10 次 |
| `PeriodicMemoryCheckerSuite` | **26/26**（第七轮后为 31/31） |
| `CacheStorageReservationDynamicSizingSuite` | **1/1** |
| 其它 Gluten C++ 测试 | 全过 |

`velox_memory_test` 在**基线上就 core dump**（`InitGoogleLogging() twice!`），
`git stash` 后重跑验证过，预先存在，与本改动无关。

## 状态

- handoff 重写为 `specs/2026-08-01-velox-admission-capacity-handoff.md`
- 设计文档 §8.1 重写：从「报告与执行分家」改为「降容瞬间的在途分配」，并说明为什么不会造成记账错误
- **Gluten 侧改动依赖上游 PR**，合并前不能提交（CI 上的 velox 还没有 `...WithCapacity`）

---

# 第七轮：子 agent 评审查出死区把对账一起过滤了

本轮只跑了一个 agent（`gpt-5.6-sol`，只看未提交改动，只报高置信度正确性 bug），
报了一条 High，**核对属实，且推翻了我在设计文档里刚写下的说法**。

## findings

> **Old-cap allocations can finish after Spark is repaid** —— `setCapacity()` 不是原子交接。
> 一个分配可以取到旧的受管容量、挂起、在 `setCapacity()` 返回之后才 `fetch_add`。
> JVM 于是按一个过时的低值还款给 Spark，`reserved` 落在实际占用之下。

前半段我在 §5 已经承认（在途窗口）。**决定性的是它的第 4 点**：

```scala
val target = nextCapacity(current, maxStorage)
if (math.abs(target - current) < deadZoneBytes) {
  return          // ← 根本没调 native
}
```

死区判断在**调 native 之前**就返回。于是：

- cache 停在**底线**上时，`target` 恒等于当前值，差为 0，**每一轮都被过滤**
- 稳态下落在死区里，同样每轮被过滤

⇒ 欠账**永远不会被发现**，Spark 一直以为那部分内存是空闲的。

我在设计文档 §8.1 写的「**延迟一轮暴露**」是错的，实际是
「延迟到某个没被过滤的轮次」，而那可能永远不来。

## 根因：一个变量装了两个量

| | 含义 | 对账时该怎样 |
|---|---|---|
| 借了多少（`reserved`） | 必须覆盖实际占用 | **上调** |
| 上界（`governed`） | AIMD 从它算下一步 | **不能跟着涨** |

原来只有 `reserved`，代码默认两者相等。我第六轮加的对账把它们撑开了，
死区又拿被撑开的值去判断，于是卡死。

而且若让上界跟着实际占用涨，一次超发就会把 cache 的上界**永久抬高**。

## 改法

1. 新增 `governed` 状态，`reserveInitial` 里初始化为初始容量（allocator 本来就在那儿）
2. AIMD 从 `governed` 算，不再从 `reserved` 算
3. **死区只过滤「移动」，不过滤「对账」**：该轮仍调 `settle(governed)`，用不变的目标再问一次
4. `grow`/`shrink` 收敛成一个 `settle(target)`，两个方向都处理
5. `governed = math.min(target, applied)` —— 取小，既拿到 native 的页对齐，又不会被超发抬高

顺带一个有意接受的副作用：`grow` 现在会把页对齐的余数（26MB 步长上约 2457 字节）**还回去**，
而不是让它借着却用不上、一直挂到 executor 结束。每次 grow 多一次 Spark 调用，换记账准确。

## 验证（红绿都做了）

- `PeriodicMemoryCheckerSuite`：**28/28**（新增 2 个；续篇再增 3 个至 31/31）
- **红**：把 `settle(governed)` 改回裸 `return`，**恰好** `settles the books even when the move
  is filtered by the dead zone` 一个失败（27/28）
- **绿**：改回来，28/28
- `spark_mmap_allocator_test` 17/17（连跑 5 次）、`CacheStorageReservationDynamicSizingSuite` 1/1

## 教训

第六轮我写下「延迟一轮暴露」时，**没有去看死区判断在调用链的哪一步**。
这是第四轮那次误判的同一种毛病：**对自己刚写的机制想当然，没有沿调用链走一遍**。

## 第七轮续：同一个毛病还有三处

把第一处修完发回 agent 复审，又报了两条 High，**核对全部属实**，而且和第一条是**同一个毛病**：
只要有任何一条路径能绕过对账，欠账就会在那条路径上被无限期挂起。

### 绕过点二、三：`grow` 的两个提前返回

```scala
if (reservation.borrow(target - reserved) > 0) { ... settle(target) }
// 借不到就什么都不做 ← 绕过点
```

- **Spark 拒绝借出**：Spark 的 storage pool 要么全给要么不给，繁忙 executor 可以每一轮都拒绝。
  于是**恰恰在 Spark 最紧张的时候**，欠账被挂起。
- **`target <= reserved`**：对账会把 `reserved` 抬到实际占用之上，于是下一个目标可能低于它，
  `target - reserved` 为负。而 `borrow` 对非正数**返回 0**（`CacheStorageReservation.scala:65-67`），
  于是上界卡死、超额预留永远还不掉。

改法：`needed <= 0` 时直接 `settle(target)`（房间已经付过钱了），借失败时 `settle(governed)`。

### 绕过点四：`stop()` 之后没有下一轮

```scala
val applied = setCapacity(reserved, 0)   // 读数返回即过期
if (reserved - applied > 0) repay(...)   // 然后漏网的分配落地
```

普通轮次可以基于一个「返回即过期」的读数结清，因为**下一轮会纠正**。`stop()` 没有下一轮。

而且方向是危险的那一侧：`reserved` **低于**实际占用 ⇒ Spark 以为内存空闲 ⇒
可能交给仍在运行的 task（Spark 关闭 task pool 时不等待）。

**我原来把这个方向判反了**，还在给 agent 的问题里说"under-reporting 是安全方向"。
agent 直接纠正：`applied > reserved` 时不借，那是 under-reservation，不是安全方向。

**改法迭代了三次，前两次都被否。**

| 试过的办法 | 为什么不行 |
|---|---|
| 两趟 `setCapacity`，第二趟收漏网的 | 只是收窄，跨越两趟仍会漏，**概率性** |
| 留住 `minCacheBytes`（= `taskSlots × loadQuantum`） | **这个上界本身就是错的**，见下 |
| **只降容量、不归还** | 最终方案 |

第二个办法我一度很得意——复用启动期 `checkFloorFitsConcurrentReads` 已经强制的不变式，
既可证明又能归还绝大部分。agent 直接用两处代码把它否了：

- **prefetch 走 connector 的 IO 线程池**，大小由 `IOThreads` 配置
  （`VeloxBackend.cc:234`，**默认等于 task slots**，但**可以调大**），预取提交到该池
  （`CachedBufferedInput.cpp:583-587`）
- ~~整文件预读一次要一整个文件，文件大小无上界~~ —— **这条是错的，见下方「后续更正」**

所以最终回到 agent 一开始就建议的做法：`setCapacity(reserved, 0)` 之后**什么都不还**。
留着的预留随 executor 退出释放，不花代价。

**顺带查出一个既有问题**（非本次引入）：`checkFloorFitsConcurrentReads` 的估算漏掉了上面两条，
底线可能不足以容纳并发读，而 `kNoCacheSpace` 致命且无人捕获。已记入设计文档 §8.2，未修。

## 红绿验证（三个新测试）

| 测试 | 红（还原修复后） |
|---|---|
| `settles the books even when Spark refuses the growth` | 失败 |
| `grows without borrowing when the room is already reserved` | 失败 |
| `catches an allocation that lands while the cache is being shut down` | 失败 |

**其中第三个的第一版写错了，是红阶段抓出来的**：`newChecker` 自己会调一次 `setCapacity`，
把我注入的"漏网分配"消耗掉了，于是单趟版本也能通过。改成在 `stop()` 前显式 arm 才真正区分开。
—— **如果没做红阶段，我会拿一个证明不了任何事的测试去声称修好了。**

另外 `stays put when Spark has nothing to lend` 原来断言 `native.targets.isEmpty`，
那是在**断言 bug**（借不到就不碰 native）。已改为断言"上界没动，但 native 被问过了"。

## 结果

`PeriodicMemoryCheckerSuite` **31/31**，`spark_mmap_allocator_test` 17/17，
`CacheStorageReservationDynamicSizingSuite` 1/1，其余 Gluten C++ 测试全过。

## 教训

第六轮我加对账时，只想着"哪里会产生欠账"，没想"哪里会**跳过**对账"。
一个横切关注点（对账）散落在多个分支里，就一定会漏掉某条分支 ——
正确的做法是让每条路径都收口到同一个 `settle`。

## 第七轮再续：agent 第三轮复审

**确认通过**：`adjust()` 的所有业务分支现在都会走到 `settle()`（只有 `maxStorage <= 0`
的 teardown 分支有意跳过）；`target <= reserved` 时抬升上界而不借钱是安全的；
`min(target, applied)` 在 pin 场景下也正确。

**仍然报了一条**：两趟 `stop()` 只是收窄而非关闭竞态。核对属实——一个跨越两趟都挂着的
分配仍会漏。agent 建议"全部保留"。

**我给了第三种做法**：留住底线（见上）。可证明覆盖，且仍归还绝大部分。
关键是发现这个类**启动时就已经**强制了 `minCache >= taskSlots × loadQuantum`
（`checkFloorFitsConcurrentReads`），而那正是"还可能落地的量"的上界 ——
现成的不变式，直接复用。

红绿：把 `keep = applied + minCacheBytes` 改回 `keep = applied`，三个 stop 测试全部失败；
改回来 31/31。

## 第七轮收尾：agent 第四轮，两条 High，方案定稿

我把「留住底线」发回复审，agent **用两处代码把这个证明拆了**（见上），另外还报了一条：

> `stop()` 只会**还**，从不**补**。若 `reserved < applied + minCacheBytes`，条件为假、直接跳过，
> `reserved` 就一直短着。

也成立——我假设了 `reserved >= keep`，但没有任何理由保证它。

两条合起来的结论：**这个窗口在 JVM 侧无法证明地关闭**，因为「还能落地多少」没有可计算的上界。
于是回到最朴素的做法：只降容量、什么都不还。

### 三次尝试的账

| | 安全性 | 归还 | 结局 |
|---|---|---|---|
| 两趟结清 | 概率性 | 全部 | 否 |
| 留住底线 | **看似**可证明 | 底线以上 | 否——上界本身是错的 |
| 只降容量不归还 | 可证明不劣化 | 无 | **采纳** |

第三种"归还无"，但代价是零：executor 正在退出，那块内存本来就随进程释放。

### 教训

我连续两次想用"聪明"的办法绕开一个本质约束（不能静默 ⇒ 不能证明），
两次都被具体代码打回。**agent 第一轮就给出了正确答案，我花了三轮才接受。**

尤其第二次：我从 `checkFloorFitsConcurrentReads` 找到一个"现成的不变式"就直接拿来用，
**没有去核对那个不变式本身是否正确**。它只在默认配置下成立（IO 线程池可以调大）——
**复用一个不变式之前，要先验证它成立。**

> 讽刺的是，紧接着我又犯了一次同样的错，只不过这次是在**接受反驳**的方向上：
> agent 给的第二条理由是错的，我没核对就写进了文档和 commit message。见下。

## 后续更正：agent 报的第二条理由是错的，我照搬了没核对

用户指出「整文件预读无上界」不成立。核对属实：

```cpp
// ReaderBase.cpp:115
if (fileLength_ <= options_.filePreloadThreshold()) {
  input_->preload();
}
```

整文件预读**有硬门槛**，只对小文件生效。阈值 Gluten 默认 **1MB**（velox 默认 8MB），
而 `loadQuantum` 强制 ≤ 8MB —— **单次预读小于一个 quantum**，`taskSlots × loadQuantum` 完全罩得住。

agent 引了 `ReaderBase.cpp:114-116`，但**没读那个 `if` 条件**，看到「一次要一整个文件」就下了结论。

**我核对时只验证了「`preload()` 确实对 `fileSize_` 做一次 `findOrCreate`」这半句，没往上追调用条件。**
这就是我这几轮反复犯的同一个毛病——只验证了断言中我预期会成立的那部分。

### 影响

| | 原判断 | 更正后 |
|---|---|---|
| 整文件预读 | 无上界 | **有门槛，被 quantum 覆盖** |
| IO 线程池 | 独立可配 ⇒ 上界不可算 | **默认等于 slots**，只有显式调大才偏 |
| 关机不归还的决定 | 「算不出上界」 | 理由减弱但**结论不变**——依赖「用户不去调某个配置」的上界不能用作安全性证明 |

### 顺带重构

把 `checkFloorFitsConcurrentReads` 和 `checkDeadZoneLeavesRoomToMove` 合并成一个
`checkConfiguration`，所有配置校验集中在一处，并把「为什么是 `taskSlots × loadQuantum`」
的完整推导（含上面两个产生方各自为什么被覆盖）写进注释。

另加一条 `minCache > 1GB` 的告警——底线是**executor 全生命周期都不还**的内存，
写大了通常是单位写错，而且不会有任何其他症状。
