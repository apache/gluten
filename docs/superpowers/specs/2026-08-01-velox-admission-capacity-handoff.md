# Handoff：Velox 的可调准入容量（`admissionCapacity`）

**状态**：上游改动已有本地原型，待提 PR
**日期**：2026-08-01（第三版）
**Velox 基线**：`IBM/velox` 分支 `dft-2026_07_28`，commit `02893c0c8`
**原型位置**：`/home/chang/OpenSource/velox`（未提交的本地修改）
**相关**：`2026-07-31-velox-cache-backpressure-design.md`（Gluten 侧设计）、`../03-review-governed-cache.md`（评审记录）

---

## 0. 一句话

`MmapAllocator` 把三条准入路径的容量提成**按值传入的参数**，派生类就能用自己的上界，
而基类的原子准入逻辑一行不改。

前两版提案（给 allocator 加 `setCapacity()`、给 cache 加 `setMaxBytes()`）都已否决，
理由见 §4。这一版**已实测跑通**：Gluten 侧只需 40 行覆写，17 个测试连跑 10 次全过。

---

## 1. 上游改动

纯重构，**行为逐字节不变**：原有入口全部传 `capacity_`。

### 1.1 头文件

```cpp
class MmapAllocator : public MemoryAllocator {
 protected:
  /// Allocates non-contiguous memory using 'admissionCapacity' as the maximum
  /// number of allocated and mapped pages.
  bool allocateNonContiguousWithCapacity(
      const SizeMix& sizeMix, Allocation& out, MachinePageCount admissionCapacity);

  bool allocateContiguousWithCapacity(
      MachinePageCount numPages, Allocation* collateral,
      ContiguousAllocation& allocation, MachinePageCount maxPages,
      MachinePageCount admissionCapacity);

  bool growContiguousWithCapacity(
      MachinePageCount increment, ContiguousAllocation& allocation,
      MachinePageCount admissionCapacity);
```

### 1.2 实现

原来的三个入口变成一行转调：

```cpp
bool MmapAllocator::allocateNonContiguousWithoutRetry(const SizeMix& sizeMix, Allocation& out) {
  return allocateNonContiguousWithCapacity(sizeMix, out, capacity_);
}
```

函数体里 `capacity_` 全部换成 `admissionCapacity`，`ensureEnoughMappedPages` 也接这个参数：

```cpp
bool MmapAllocator::ensureEnoughMappedPages(
    int32_t newMappedNeeded, MachinePageCount admissionCapacity);
```

**按值传参是关键**：`ensureEnoughMappedPages` 里 `totalMaps <= admissionCapacity` 和
`totalMaps - admissionCapacity` 是两处独立使用，若改成读一个可变成员，
两次读之间容量升高会让这个 `uint64_t` 减法**下溢成 ~2^64**。按值传入从设计上避开了。

`capacity_`、`capacity()`、`MemoryManager`、`arbitrator` 全部不动。

---

## 2. 使用方（Gluten）

覆写三个私有虚函数，转调 protected 版本：

```cpp
class SparkMmapAllocator : public MmapAllocator {
 private:
  bool allocateNonContiguousWithoutRetry(const SizeMix& sizeMix, Allocation& out) override {
    return allocateNonContiguousWithCapacity(sizeMix, out, governedPages());
  }
  // allocateContiguousWithoutRetry / growContiguousWithoutRetry 同形

  MachinePageCount governedPages() const {
    return AllocationTraits::numPages(governedCapacity());   // 一个 atomic 的 relaxed load
  }
};
```

私有虚函数**允许覆写**（只是不能从外部调用），分配体留在基类。

---

## 3. 为什么这样是对的

### 3.1 精确性来自 velox 自己的原子准入

```cpp
if (numAllocated_.fetch_add(pages) + pages > admissionCapacity) {
  numAllocated_.fetch_sub(pages);     // 放不下就原样退回
  return false;
}
```

**用的是 `fetch_add` 自己的返回值**，不是重新读。硬件保证两个线程拿不到同一个返回值，
所以"都以为还有空间"从物理上不会发生。我们只换了比较对象，判断逻辑一字未动。

### 3.2 与只覆写 `capacity()` 的区别

| | 只覆写 `capacity()` | 传 `admissionCapacity` |
|---|---|---|
| `canTryAllocate` 看到 | 受管值 | 受管值 |
| **真正发页时比的** | **`capacity_`（配置值）** | **受管值** |
| 实测稳态超出 | 8KB / 32MB | **0** |

前者是"门口写着限 32MB，里面按 64MB 放行"，而 `canTryAllocate` 又是**无锁**的
（`AsyncDataCache.cpp:906` 注释自陈是有意为之），两个读者同时过门，里面照单全收。

### 3.3 实测

16 线程 × 3000 × 8KB entry，授予 32MB（`SparkMmapAllocatorTest`）：

| | 峰值超出 |
|---|---|
| `cachedPages()`（真实缓存页） | **0**，三次全部 |
| `numAllocated_`（计数器） | +8KB |

计数器那 8KB 是 `fetch_add` 与 `fetch_sub` 之间被别的线程读到的**正在被拒绝的请求**，
不是发出去的内存。用 `cachedPages()`（只在分配**成功后**才更新）做对照测出来的。

> 这一条我先凭推理判断过一次并且判错了，这次是拿探针测的。

### 3.4 性能：零开销

`BM_CacheAllocate`（一次完整 cache 分配 + 释放）：

```
base       348 ns
governed   348 ns
```

一个 relaxed atomic load（0.22ns）被 348ns 淹没。

顺带测了"加读写锁关死降容窗口"的代价，作为否决依据：

| 线程数 | 裸原子 | `folly::SharedMutex` | `std::shared_mutex` |
|---|---|---|---|
| 1 | 0.22 ns | 9.8 ns | 11.6 ns |
| 16 | 0.24 ns | 11.1 ns | **1482 ns** |

`std::shared_mutex` 的读锁内部是同一 cache line 上的原子 RMW，读者互相打架，
16 线程下比整个分配还贵 4 倍 —— 排除。folly 版约 +3.2%，但换来的只是"少延迟一轮记账"（§5），
不划算。

---

## 4. 前两版为什么否决

### 4.1 第一版：给 `MemoryAllocator` 加 `setCapacity()`

| 问题 | 证据 |
|---|---|
| 打破 `MemoryManager` / arbitrator 不变式 | `Memory.cpp:167` 只在**构造时**校验 `allocator_->capacity() >= arbitrator_->capacity()`；`Memory.cpp:253` `MemoryManager::capacity()` 直接返回 `allocator_->capacity()`。改 allocator 容量 ⇒ MemoryManager 报告跟着变，arbitrator 毫不知情 —— **正是本诉求想解决的问题的镜像** |
| clamp 消不掉下溢 | `numAllocated()` 在 clamp 后随时会涨，`AsyncDataCache.cpp:1046-1049` 与 `CachedBufferedInput.cpp:100-101` 的无符号减法照样回绕 |
| `MallocAllocator` 的 `capacity_ == 0` 表示无限 | `MallocAllocator.h:186-187` 注释原话 |
| 加纯虚会打断树外实现 | `MemoryAllocator.h:212-213`「Proxy subclasses may provide context specific tracking」是公开扩展点 |

**`admissionCapacity` 把这四条全部避开**：不碰 `capacity_`、不碰 `capacity()`、
只动 `MmapAllocator`、不加虚函数。

### 4.2 第二版：给 `AsyncDataCache` 加 `setMaxBytes()`

把上界放在 cache 的 `cachedPages_` 上，两个分配器不动。方向对，但：

- 准入点 `AsyncDataCacheEntry::initialize()` 是**调进** `MemoryAllocator::allocateNonContiguous`
  的 `makeSpace` 重试环里的（`MemoryAllocator.cpp:224-233`）。上界若在调分配器**之前**拦截，
  失败到不了 `makeSpace`，**淘汰不会被触发**，直接 `VELOX_CACHE_ERROR` → `kNoCacheSpace`。
- `kNoCacheSpace` 全仓库只有 `exec/fuzzer/CacheFuzzer.cpp:497` 一处 catch，**生产路径无人接**。
- 后果：「cache 到达上界」这个**稳态**会让之后每一次 miss 都抛未捕获的致命错误。

要修就得让 `canTryAllocate` 也认这个上界 —— 那又回到"报告与执行两条线"。
`admissionCapacity` 直接在分配器里拒绝，失败被 `makeSpace` 接住并转入淘汰，路径本来就是通的。

### 4.3 顺带记：整类拷贝进 Gluten 是**编译期不可行**的

`Allocation.h:193-195` 只把 `append()` / `clear()` 的权限发给三个指名道姓的 friend 类，
而 **C++ 友元不继承**。要拷贝就必须往 velox 里加 `friend class ::gluten::SparkMmapAllocator;`
—— 在 velox 里写死 Gluten 类名，永远上不了游。

`admissionCapacity` 方案下**分配体留在基类**，这个死结不存在。

---

## 5. 唯一残留的窗口：降容瞬间

`admissionCapacity` 是**入口按值取的快照**，所以已经进入函数的线程比的是旧值。

- **稳态**：精确，实测超出 0。
- **降容瞬间**：在途线程按旧容量放行。上界 = 在途请求之和 ≤ `并发数 × loadQuantum`
  （loadQuantum 开 cache 时被强制 ≤ 8MB）。

这个窗口**关不死**，除非把容量和已分配量塞进同一个原子字，或者上锁（velox 明确避免，
且实测代价见 §3.4）。

但它**不会造成记账错误**，因为：

1. 发出去的页一定进 `numAllocated_`（`fetch_add` 在检查**之前**）
2. `capacity()` 返回 `max(governed, allocatedBytes)`，所以超发**藏不住**
3. JVM 侧拿 `setCapacity` 的返回值补账（见 §6）
4. 同时 cache 的余量已经是 0，新分配全被挡，淘汰追上后自动收敛

性质是"**下一轮结清**"，不是"永久失账"——但前提是**结清独立于 AIMD 的决策**，
否则 cache 停在死区或底线上时每轮都被过滤，欠账永远不会被发现。这一点第一版漏了，见 §6.2。

---

## 6. Gluten 侧的配套改动

### 6.1 `setCapacity` 从"拒绝"改成"如实上报"

原来是：

```cpp
VELOX_CHECK_GE(from, capacity(), "Cache is holding more memory than has been reserved for it");
```

**这个 assert 恰好堵死了唯一的出路。** 撞上 §5 那个窗口后它抛，`onTick` 接住，
**容量根本没降下去** —— 而降容量正是逼 cache 淘汰、把内存吐出来的唯一手段。
于是每一轮都抛，cache 永远出不来。

改成如实存、如实报，让调用方去补账。

### 6.2 JVM 侧对账，且**独立于 AIMD 的决策**

两个量必须分开，早期版本把它们混在一个 `reserved` 里，这是错的：

| | 含义 | 对账时 |
|---|---|---|
| `reserved` | 向 Spark 借了多少，必须覆盖实际占用 | **上调** |
| `governed` | 上次设下的上界，AIMD 从它算下一步 | **不能跟着涨** |

若让 `governed` 跟着实际占用涨，一次超发就会把 cache 的上界**永久抬高**。

对账收在一个 `settle(target)` 里，两个方向都处理：

```scala
private def settle(target: Long): Unit = {
  val applied = setCapacity(reserved, target)
  governed = math.min(target, applied)     // 取小：既拿到 native 的页对齐，又不被超发抬高
  if (applied < reserved) {
    reservation.repay(reserved - applied); reserved = applied
  } else if (applied > reserved && reservation.borrow(applied - reserved) > 0) {
    reserved = applied
  }
}
```

**最容易漏的一点：对账必须在每一条路径上都发生。**

只要有任何一个分支能绕过 `settle`，欠账就会在那个分支上被无限期挂起。
子 agent 评审连续查出三处，全都是同一个毛病：

| 绕过点 | 后果 |
|---|---|
| 死区判断直接 `return` | cache 停在死区、尤其坐在**底线**上（target 恒等于当前值，差为 0，**每轮必被过滤**）时，欠账永远不会被发现 |
| Spark 拒绝借出，`grow` 直接返回 | 恰恰在 Spark 最紧张的时候把欠账挂起 |
| `target <= reserved` 时 `borrow(负数)` 返回 0 | `borrow` 对非正数返回 0（`CacheStorageReservation.scala:65-67`），于是上界卡死、超额预留永远还不掉 |

现在每条路径都收口到 `settle`：

```scala
// adjust
if (math.abs(target - governed) < deadZoneBytes) { settle(governed); return }

// grow
val needed = target - reserved
if (needed <= 0) { settle(target); return }                        // room already paid for
if (reservation.borrow(needed) > 0) { reserved = target; settle(target) }
else { settle(governed) }                                          // 拒绝也要对账
```

### 6.3 `stop()` 什么都不做

`stop()` 之后**再没有下一轮**了。普通轮次的对账之所以可以基于一个「返回即过期」的读数，
正是因为下一轮会纠正它。

而且方向是危险的那一侧：`reserved` **低于**实际占用，意味着 Spark 以为那块内存是空闲的，
可能交给一个仍在运行的 task（Spark 关闭 task pool 时不等待）。

**还能落地多少，从 JVM 这边算不出来。** 试过两个"聪明"的办法，都被评审否掉：

| 试过的办法 | 为什么不行 |
|---|---|
| 跑两趟 `setCapacity`，第二趟收漏网的 | 只是收窄，跨越两趟仍会漏，是**概率性**的 |
| 留住 `minCacheBytes`（= `taskSlots × loadQuantum`） | 这个上界依赖「用户不去调 `IOThreads`」，不能用作安全性证明 |

那**只降容量、不归还**呢？也不值得：

- 会把**仍在运行的任务马上要读的 entry** 淘汰掉
- 释放的内存是还给**操作系统**，不是还给 Spark（预留还挂着，别人拿不到）
- 而进程下一刻就退出，那块内存本来就要还

所以 `stop()` 只做一件事：**停掉调节线程**。留着的预留随 executor 消失，不花任何代价。

---

## 7. 验证

| | 结果 |
|---|---|
| `spark_mmap_allocator_test` | **17/17**，连跑 10 次 |
| `PeriodicMemoryCheckerSuite` | **31/31** |
| `CacheStorageReservationDynamicSizingSuite` | **1/1** |
| `BM_CacheAllocate` | base 348ns / governed 348ns，**无差异** |

关键是那个原本记录着洞的测试。它的注释白纸黑字写着：

> *"The allocator itself still enforces only its static capacity, so this call succeeds"*

现在断言的是相反的事 —— 超出授予容量的分配被**分配器当场拒绝**，`allocatedBytes()` 保持 0。

Scala 侧同理，原来叫 `cannot yield while the cache is over its reservation`（记录卡死），
现在是 `settles up when the cache is over its reservation` +
`keeps shrinking once the pinned entries are released`（**卡死变自愈**）。

---

## 8. 待上游确认

1. **`admissionCapacity` 要不要也给 `MallocAllocator`？** 目前只有 `MmapAllocator` 有用例。
   注意 `MallocAllocator` 的 `capacity_ == 0` 表示无限（`MallocAllocator.h:186`），
   语义需要单独想清楚。
2. **`allocateBytesWithoutRetry` 的 malloc 分支不受管**（`MmapAllocator.cpp:482-491`
   记的是 `numMallocBytes_`）。对 cache 无影响（`kTinyDataSize` 以下的 entry 在
   `AsyncDataCache.cpp:146-149` 根本不到分配器），但接口语义上要说明。
3. **两处无符号减法**（`AsyncDataCache.cpp:1046-1049`、`CachedBufferedInput.cpp:100-101`）
   建议改成饱和减法。今天不回绕只因为 `capacity()` 恒定，是个独立的小补丁。

---

## 9. Gluten 侧的收尾

上游 PR 合并、daily update 带进来之后：

1. 移除本文档，把 §5 的窗口写进设计文档 §8
2. `SparkMmapAllocatorTest::holdsTheGrantWhileReadersFillTheCacheTogether`
   的计数器容差可以留着（那是 velox 固有的瞬时值），但注释要说明它测的是计数器不是内存

**在那之前，Gluten 侧这部分改动不能提交** —— CI 上的 velox 还没有 `...WithCapacity`。

---

## 附：可直接贴到 Velox PR 的英文说明

> ### Let a subclass choose the capacity an allocation is admitted against
>
> **What**
>
> Pull the bound out of the three admission paths in `MmapAllocator` into a
> by-value parameter, and expose them as protected:
>
> ```cpp
> bool allocateNonContiguousWithCapacity(const SizeMix&, Allocation&, MachinePageCount admissionCapacity);
> bool allocateContiguousWithCapacity(..., MachinePageCount admissionCapacity);
> bool growContiguousWithCapacity(..., MachinePageCount admissionCapacity);
> ```
>
> The existing entry points become one-line forwarders passing `capacity_`, so
> this is a pure refactor: behaviour is unchanged to the byte, and
> `capacity_`, `capacity()`, `MemoryManager` and the arbitrator are untouched.
>
> **Why**
>
> Apache Gluten runs `AsyncDataCache` inside a Spark executor. Spark manages
> off-heap memory by quota, but the cache is invisible to it, so an executor
> occupies its quota *plus* the entire cache and Spark's spill and OOM decisions
> are made on wrong numbers. Governing it means moving the bound as Spark's own
> demand moves.
>
> Overriding the virtual `capacity()` is not enough. That only changes what
> `AsyncDataCache::canTryAllocate()` reads, and that check is deliberately
> unlocked (`AsyncDataCache.cpp:906`), while admission is still enforced against
> the const `capacity_`. Readers that pass the check together then take more than
> the bound: we measured 8KB of overshoot on a 32MB cap with 16 threads.
>
> Reimplementing the admission path in a subclass is not an option either — the
> body reaches nine private members — and copying `MmapAllocator` out of Velox
> does not compile, because `Allocation::append()` and `Allocation::clear()` are
> private to three named friend classes and friendship is not inherited.
>
> **Notes**
>
> - The bound is passed by value on purpose. `ensureEnoughMappedPages()` uses it
>   twice (`totalMaps <= capacity` and `totalMaps - capacity`), and those are
>   `uint64_t`, so reading a mutable member twice would underflow if the capacity
>   rose in between.
> - A subclass that lowers the bound should keep `capacity()` at or above
>   `numAllocated()`. Callers compute headroom as
>   `numPages(capacity()) - numAllocated()` in unsigned arithmetic
>   (`AsyncDataCache::canTryAllocate`, `CachedBufferedInput::shouldPreload`), so a
>   capacity below the allocated bytes wraps around and removes the bound
>   entirely. Those two sites would be better written as saturating subtraction
>   regardless.
> - With the bound lowered, `numAllocated_` still counts every page handed out
>   (the `fetch_add` precedes the check), so an overshoot cannot hide from a
>   subclass reporting usage back to its host.
>
> Measured cost: none. `BM_CacheAllocate` is 348ns either way — the extra atomic
> load is 0.22ns.
