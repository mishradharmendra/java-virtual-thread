# Platform Threads vs. Virtual Threads + Structured Concurrency

### A measured comparison, using one identical workload

This document compares the two concurrency models implemented in this repository. Both run the
**exact same producer/consumer workload** — the same `OrderProducer` and `OrderConsumer` instances,
the same quota, the same simulated I/O — and differ *only* in how those workers are scheduled and
supervised.

Every number below was measured on the machine described in [§2](#2-methodology). Nothing is quoted
from a blog post; every table can be reproduced with the commands in [§13](#13-reproducing-these-measurements).

---

## Table of contents

1. [Executive summary](#1-executive-summary)
2. [Methodology](#2-methodology)
3. [Mental model: what a thread actually is](#3-mental-model-what-a-thread-actually-is)
4. [Memory](#4-memory)
5. [Resource utilisation](#5-resource-utilisation)
6. [Scalability and the hard ceiling](#6-scalability-and-the-hard-ceiling)
7. [Ease of use: lifecycle management](#7-ease-of-use-lifecycle-management)
8. [Error handling and cancellation](#8-error-handling-and-cancellation)
9. [Nested parallelism and thread-starvation deadlock](#9-nested-parallelism-and-thread-starvation-deadlock)
10. [Timeouts](#10-timeouts)
11. [Observability and debugging](#11-observability-and-debugging)
12. [When *not* to use virtual threads](#12-when-not-to-use-virtual-threads)
13. [Reproducing these measurements](#13-reproducing-these-measurements)
14. [Decision matrix and migration checklist](#14-decision-matrix-and-migration-checklist)
15. [Glossary](#15-glossary)

---

## 1. Executive summary

Identical workload: **900 workers** (300 producers + 600 consumers) fulfilling **2,000 orders**.
All figures below are from a **single paired run** so the CPU counters are directly comparable.

| Aspect | Legacy (platform threads) | Modern (virtual + structured) | Advantage |
|---|---:|---:|---|
| Wall-clock time | 426 ms | **388 ms** | 1.10× faster |
| Throughput | 4,695 orders/s | **5,155 orders/s** | 1.10× |
| **Peak OS threads** | 2,106 | **27** | **78× fewer** |
| **Peak RSS** | 254.9 MB | **95.3 MB** | **2.7× less** |
| Peak memory footprint | 232.7 MB | **70.1 MB** | **3.3× less** |
| Instructions retired | 3.523 B | 3.665 B | ~equal (same work) |
| **CPU cycles elapsed** | **9.702 B** | **3.222 B** | **3.0× fewer** |
| **Instructions per cycle** | **0.36** | **1.14** | **3.1× better** |
| Involuntary context switches | 39,739 | **25,699** | 1.5× fewer |
| Engine source (effective LOC) | 134 | **105** | 22% less |
| Nested fan-out (effective LOC) | 66 | **35** | 47% less |

Scale the same workload to **2,500 workers**:

| | Legacy | Modern |
|---|---|---|
| Result | 💥 **`OutOfMemoryError: unable to create native thread`** | ✅ 244 ms, 12,295 orders/s |
| Peak OS threads | — (hit the 6,116 ceiling) | **27** |
| CPU cycles | 52.4 B *(burned while failing)* | **4.0 B** |

At **12,000 workers** — twice the platform-thread ceiling on this machine — the modern engine
fulfils 20,000 orders in **393 ms at 50,891 orders/s on 27 OS threads**. The legacy model cannot
express this configuration at all.

**The headline finding is not speed.** On a small workload the two are within 10% of each other.
The findings that matter are:

1. **Same instructions, 3× the cycles.** The legacy engine actually retires *slightly fewer*
   instructions (3.523 B vs 3.665 B) — the work is genuinely identical — yet needs **3.0× more CPU
   cycles** to retire them (IPC 0.36 vs 1.14). Those lost cycles are pure overhead: cache and TLB
   thrashing across 2,106 thread stacks, plus kernel scheduler work. You pay ~3× the CPU for the same
   business outcome.
2. **The legacy model has a wall; the modern one does not.** 6,116 threads on this machine, and then
   the process dies.
3. **The modern code is smaller and has fewer failure modes**, which is the benefit you feel every
   day, long before you feel the performance one.

---

## 2. Methodology

**Environment**

| | |
|---|---|
| Machine | Apple Silicon (arm64), 18 cores |
| OS | macOS 26.6.2 |
| JDK | Amazon Corretto 26.0.2.1+11 |
| `kern.num_taskthreads` | 6,144 (max threads per process) |
| `ulimit -u` | 4,000 |
| JVM flags | `--enable-preview` (StructuredTaskScope is preview in JDK 26) |

**Controls applied**

- Each measurement runs in its **own JVM**, so peak thread count and RSS are not polluted by a
  previous run.
- RSS and CPU counters come from `/usr/bin/time -l`, i.e. the OS, not the JVM's self-report. This
  matters enormously: a platform thread's stack is *native* memory that never appears in heap
  statistics. Measuring only the Java heap would make platform threads look **cheaper** than virtual
  threads, which is exactly backwards.
- Both engines execute the same `WarehouseWorker` instances built by the same `ShiftAssembly`. The
  business logic is not merely equivalent — it is the same code.
- Quality checks use a strategy (`QualityInspector`) so both engines perform identical nested work.

**Honest caveats**

- These are *simulation* workloads using `Thread.sleep` to model I/O. That is representative of
  blocking network/disk calls, which is where virtual threads apply. It is **not** representative of
  CPU-bound work — see [§12](#12-when-not-to-use-virtual-threads).
- Wall-clock figures on a laptop have noise in the ±10% range. The thread-count, memory and cycle
  figures are stable and are the ones to take seriously.
- `Thread.sleep` on a virtual thread unmounts the carrier — the same as a blocking socket read in
  JDK 21+. Sleeping on a platform thread blocks the OS thread. That asymmetry is not a measurement
  artefact; it is precisely the property under test.

---

## 3. Mental model: what a thread actually is

Almost every difference below follows from one design decision.

### Platform thread — a 1:1 wrapper around an OS thread

```
java.lang.Thread  ──────────►  OS thread (pthread)
                                 ├── fixed stack, reserved at creation (default 1 MB, -Xss)
                                 ├── kernel scheduling entity
                                 └── kernel-managed context (registers, signal mask, TLS)
```

- **Creation** is a syscall. Measured: **39–94 µs each**, and it gets *worse* with scale (39 µs at
  1,000 threads, 87 µs at 5,000) as the kernel's per-process tables grow.
- **Blocking** parks the OS thread. The kernel is involved, and the thread is unavailable for other
  work while it waits.
- **Count is capped** by the OS, not by your application's needs.

### Virtual thread — a heap object scheduled by the JVM

```
java.lang.Thread (virtual)  ──►  Continuation + StackChunk  (objects on the Java heap)
                                        │
                                        │ mount / unmount  (pure user-mode, no syscall)
                                        ▼
                                  carrier thread (a platform thread in a ForkJoinPool,
                                                  one per core by default)
```

- **Creation** is an object allocation. Measured: **0.5–15 µs each**, and it gets *better* with
  scale (14.6 µs at 1,000; 0.5 µs at 1,000,000) as JIT warms up and fixed costs amortise.
- **Blocking** copies the stack into the heap and *unmounts* the carrier, which immediately runs
  another virtual thread. No kernel involvement.
- **The stack grows and shrinks on demand.** A thread that is 3 frames deep costs ~800 bytes, not
  1 MB.
- **Count is capped by heap**, which you control, not by a kernel table you do not.

> The single sentence version: **a platform thread reserves a megabyte of stack in case you need it;
> a virtual thread allocates only the frames you actually pushed.**

---

## 4. Memory

### 4.1 Measured: raw thread cost

Peak RSS of a JVM that creates *N* threads and parks every one of them (the state a blocked worker
is in). Baseline JVM ≈ 49–52 MB.

| Threads | Platform RSS | Platform OS threads | Virtual RSS | Virtual OS threads |
|---:|---:|---:|---:|---:|
| 10 *(baseline)* | 51.7 MB | 16 | 48.6 MB | 10 |
| 1,000 | 135.9 MB | 1,006 | **59.4 MB** | **25** |
| 2,000 | 177.9 MB | 2,006 | **64.3 MB** | **25** |
| 5,000 | 314.3 MB | 5,006 | **75.0 MB** | **25** |
| 10,000 | 💥 **impossible** | — | **90.1 MB** | **25** |
| 100,000 | 💥 impossible | — | **211.1 MB** | **25** |
| 1,000,000 | 💥 impossible | — | **1,149 MB** | **25** |

**Marginal cost per thread**, computed from the slope (removes JVM baseline):

| | Marginal RSS per thread |
|---|---:|
| Platform | **≈ 53.8 KB** |
| Virtual | **≈ 1.13 KB** |
| **Ratio** | **≈ 48× cheaper** |

Java-heap cost per virtual thread, self-reported, *falls* as you scale — fixed overhead amortises:

| Virtual threads | Heap bytes/thread |
|---:|---:|
| 1,000 | 2,584 |
| 10,000 | 1,730 |
| 100,000 | 886 |
| 1,000,000 | **822** |

### 4.2 Why the gap exists

A platform thread's stack is **reserved** virtual address space (1 MB by default on 64-bit), and
**committed** lazily page by page as frames are pushed. Our probe threads are shallow, so we measured
~54 KB resident, not 1 MB. Two consequences people routinely get wrong:

- **You cannot see this in heap metrics.** Notice in the engine benchmark that the *legacy* run
  reports a **smaller Java heap** (9.2 MB vs 20.5 MB) while using **2.7× more actual memory**
  (254.9 MB vs 95.3 MB RSS). Virtual thread stacks live on the heap and are therefore visible;
  platform thread stacks are native and are not. Monitoring only heap will lead you to the wrong
  conclusion.
- **Deep stacks are far worse than our probe suggests.** A real request handler 60 frames deep
  commits considerably more. Our 54 KB figure is a *floor* for platform threads. The virtual thread
  figure scales with actual depth, so it stays proportional to real usage.

### 4.3 Worked example: a request-per-thread web service

10,000 concurrent in-flight requests, each blocked on a downstream call:

| | Platform threads | Virtual threads |
|---|---|---|
| Threads needed | 10,000 | 10,000 |
| Possible on this machine? | **No** — 6,116 ceiling | Yes |
| Memory (at measured 53.8 KB / 1.13 KB) | ~538 MB *(if it were possible)* | **~11 MB** |
| Typical real-world workaround | Cap the pool at ~200 and queue the rest | None needed |

And that workaround is the real cost. Once you cap the pool, **queueing latency becomes your p99**,
and you are forced into async/reactive code to claw throughput back — trading a memory problem for a
permanent maintainability problem.

---

## 5. Resource utilisation

### 5.1 OS threads: 78× fewer, for identical output

900 workers, 2,000 orders, both engines fulfil all 2,000:

| Metric | Legacy | Modern |
|---|---:|---:|
| Workers | 900 | 900 |
| Orders fulfilled | 2,000 | 2,000 |
| **Peak OS threads** | **2,106** | **27** |
| OS threads per worker | 2.33 | **0.02** |

Where does 2,106 come from? The legacy model needs a real OS thread for every concurrent activity:

```
  900  worker threads      (Executors.newFixedThreadPool(producers + consumers))
+ 1200  QC check threads   (ExecutorQualityInspector: consumers × 2 checks)
+    6  JVM internals      (GC, JIT, signal dispatcher, …)
───────
  2,106
```

The modern engine's 27 = JVM internals + a `ForkJoinPool` of carriers sized to the core count. The
900 workers and their 1,800 nested QC tasks are **heap objects multiplexed onto those carriers.**

> **This is the load-bearing insight of the whole comparison.** In the legacy model, "concurrency"
> and "OS threads" are the same number. In the modern model they are decoupled: concurrency is
> limited by your heap and your problem, not by a kernel table.

### 5.2 CPU: same instructions, 3× the cycles

This is the most interesting result in the document. Both counters come from a single paired run.

| Metric | Legacy | Modern | Ratio |
|---|---:|---:|---|
| Instructions retired | 3.523 B | 3.665 B | **0.96× — legacy retires slightly *fewer*** |
| **CPU cycles elapsed** | **9.702 B** | **3.222 B** | **3.0×** |
| **Instructions per cycle (IPC)** | **0.36** | **1.14** | **3.1×** |
| Involuntary context switches | 39,739 | 25,699 | 1.5× |
| Voluntary context switches | 3 | 27 | — |

Both engines execute the same amount of work — the near-identical instruction counts prove it. The
legacy engine actually retires 4% *fewer* instructions, and still needs **three times as many CPU
cycles** to get through them.

The lost cycles are **stalls**, and they come from:

- **Cache and TLB thrashing.** 2,106 live stacks, each touching its own pages, evict each other from
  L1/L2 and blow out the TLB. 27 carrier stacks stay hot in cache.
- **Kernel scheduler overhead.** The OS must consider 2,106 runnable entities instead of 27, and
  every block/unblock is a kernel transition. A virtual thread's mount/unmount is a user-mode
  continuation switch with no syscall at all.
- **Memory bandwidth** spent copying kernel thread contexts.

**Practical translation:** on cloud instances billed by vCPU, the legacy model costs roughly **3× the
CPU for the same business throughput** on blocking workloads. That cost is invisible in
application-level metrics — throughput looks similar — and shows up only on the bill.

### 5.3 Thread creation cost, and how it scales

| Threads | Platform | Virtual |
|---:|---:|---:|
| 1,000 | 39 ms (39.1 µs each) | **15 ms (14.6 µs each)** |
| 2,000 | 84 ms (42.2 µs each) | **17 ms (8.7 µs each)** |
| 5,000 | 433 ms (86.6 µs each) | **23 ms (4.6 µs each)** |
| 100,000 | impossible | **101 ms (1.0 µs each)** |
| 1,000,000 | impossible | **493 ms (0.5 µs each)** |

Note the direction of travel. **Platform threads get more expensive per thread as you add them**
(39 µs → 87 µs, a 2.2× degradation). **Virtual threads get cheaper** (14.6 µs → 0.5 µs, a 29×
improvement) as JIT warms and fixed costs amortise.

This is why thread *pools* exist: pooling is a workaround for creation being expensive. When creation
costs 0.5 µs, **pooling stops being necessary** — and with it goes pool sizing, pool tuning, pool
starvation, and thread-local leakage between pooled tasks.

---

## 6. Scalability and the hard ceiling

### 6.1 The wall is real, and closer than you think

```
$ java ... ThreadFootprintBenchmark platform 20000

created             : 6,116  <-- HIT THE CEILING
failure             : java.lang.OutOfMemoryError: unable to create native thread:
                      possibly out of memory or process/resource limits reached
```

**6,116 threads.** Not a heap limit — `kern.num_taskthreads` is 6,144 and the JVM already held 28.
No amount of `-Xmx` helps. It is an OS resource limit, and you hit it as a `OutOfMemoryError` that
is not actually about memory.

### 6.2 The same wall, in the application

Scaling the warehouse to 2,500 workers (which needs 2,500 worker + 4,000 QC = 6,500 platform threads):

| | Legacy | Modern |
|---|---|---|
| Outcome | 💥 `OutOfMemoryError: unable to create native thread` | ✅ CLEAN |
| Elapsed | — | **244 ms** |
| Throughput | — | **12,295 orders/s** |
| Peak OS threads | — | **27** |
| Peak RSS | 553.8 MB *(while failing)* | **115.5 MB** |
| **CPU cycles** | **52.4 B** | **4.0 B** |

The legacy engine burned **13× more CPU than the successful modern run consumed — in order to fail.**
Thread creation is expensive whether or not it succeeds.

### 6.3 Beyond the wall

12,000 workers, 20,000 orders, modern engine only (the legacy model cannot express this):

```
workers             : 12,000 (2,000 producers + 10,000 consumers)
orders fulfilled    : 20,000
elapsed             : 393 ms
throughput          : 50,891 orders/s
OS threads PEAK     : 27          <-- still 27
heap in use after   : 128.1 MB
maximum RSS         : 244 MB
outcome             : CLEAN
```

Twice the platform-thread ceiling, at **10× the throughput of the 900-worker run**, on the same 27
OS threads.

### 6.4 Why pool sizing is a trap (Little's Law)

For the legacy model you must choose a pool size up front. Little's Law tells you what it should be:

```
threads_needed  =  arrival_rate  ×  average_latency
```

At 1,000 req/s with 200 ms latency: `1000 × 0.2` = **200 threads**. Fine.

Now a downstream dependency slows to 2 s — a routine incident:

```
threads_needed  =  1000 × 2.0  =  2,000 threads
```

Your 200-thread pool is 10× undersized. Requests queue. Queue time adds to latency, which raises the
thread requirement further. **This is a death spiral, and it is triggered by someone else's outage.**

Your options in the legacy model are all bad:
- Size for the worst case → thousands of mostly-idle threads, and you may exceed the OS ceiling.
- Size for the normal case → collapse under any latency excursion.
- Go reactive → correct, but you rewrite the codebase in a style that is materially harder to read,
  debug, and profile.

With virtual threads the concurrency limit is heap, so the pool simply is not the constraint. You
still apply back-pressure — but deliberately, with a `Semaphore` sized to the *downstream's* real
capacity, rather than accidentally via a thread count that was chosen for unrelated reasons.

> `OrderConveyor` in this repo does exactly that: a bounded queue with a **timed** `offer()`, so
> back-pressure is explicit load-shedding (`recordRejected()`) rather than an invisible stall.

---

## 7. Ease of use: lifecycle management

Performance is the argument that gets quoted. **Lifecycle management is the one you feel daily.**

### 7.1 Side by side — the same shift, both engines

**Legacy** (`LegacyWarehouseEngine`, 134 effective LOC):

```java
try (var inspector = new ExecutorQualityInspector(config.consumers())) {   // (1) second pool
    ShiftAssembly assembly = ShiftAssembly.create(config, ENGINE_PREFIX, inspector, clock);

    ExecutorService pool =
            Executors.newFixedThreadPool(config.totalWorkers(), threadFactory());  // (2) guess a size

    List<Submission> submissions = new ArrayList<>(config.totalWorkers());
    boolean timedOut;
    try {
        for (WarehouseWorker worker : assembly.workers()) {
            submissions.add(new Submission(worker, pool.submit(worker)));   // (3) track every future
        }
        timedOut = !awaitOrderlyShutdown(pool);
    } finally {
        if (!pool.isTerminated()) {
            pool.shutdownNow();                                            // (4) belt and braces
        }
    }
    ...
}

// (5) a whole method just to shut down
private boolean awaitOrderlyShutdown(ExecutorService pool) throws InterruptedException {
    pool.shutdown();
    if (pool.awaitTermination(config.shiftTimeout().toNanos(), TimeUnit.NANOSECONDS)) {
        return true;
    }
    reporter.note("Shift timeout exceeded — escalating to shutdownNow()");
    pool.shutdownNow();
    if (!pool.awaitTermination(FORCED_SHUTDOWN_GRACE.toNanos(), TimeUnit.NANOSECONDS)) {
        reporter.note("Pool still alive after shutdownNow() — workers are ignoring interrupts");
    }
    return false;
}

// (6) and another to drain results, because nothing links the futures together
for (Submission submission : submissions) {
    try {
        submission.future().get();
        completed.merge(worker.role(), 1, Integer::sum);
    } catch (CancellationException e) {
        failures.add(worker.name() + " was cancelled during shutdown");
    } catch (ExecutionException e) {
        failures.add(worker.name() + " failed: " + describe(e.getCause()));
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
    }
}
```

**Modern** (`ModernWarehouseEngine`, 105 effective LOC):

```java
var inspector = new StructuredQualityInspector();        // nothing to size, nothing to close
ShiftAssembly assembly = ShiftAssembly.create(config, ENGINE_PREFIX, inspector, clock);

try (var scope = StructuredTaskScope.open(
        Joiner.<Void>awaitAll(),
        cf -> cf.withName(SCOPE_NAME)
                .withThreadFactory(virtualThreadFactory())
                .withTimeout(config.shiftTimeout()))) {   // timeout is configuration, not code

    for (WarehouseWorker worker : assembly.workers()) {
        forked.add(new Forked(worker, scope.fork(worker)));
    }

    try {
        scope.join();
    } catch (StructuredTaskScope.TimeoutException e) {
        timedOut = true;
        reporter.note("Shift timeout — scope cancelled every outstanding subtask");
    }

    tally(forked, completed, failures);
}
// Scope closed: every virtual thread has terminated. Guaranteed by the language, not by review.
```

### 7.2 What disappeared

| Concern | Legacy | Modern |
|---|---|---|
| Choose a pool size | Required, and wrong either way | **Not applicable** |
| Shut the pool down | `shutdown()` → `awaitTermination()` → `shutdownNow()` → await again | **`}`** (end of try-with-resources) |
| Guarantee no thread outlives the method | A `finally` block you must remember | **Structurally impossible to violate** |
| Enforce a timeout | Hand-written supervisor polling a counter every 50 ms | `.withTimeout(...)` |
| Second pool for nested work | Mandatory, to avoid deadlock | **Not applicable** |
| Attribute a failure to a worker | Manual `Future` ↔ worker pairing | Manual pairing (same) |

**The safety property is the real prize.** In the legacy engine, a thread outliving `runShift()` is
prevented by a `finally` block — i.e. by a human remembering. In the modern engine it is prevented by
the *closing brace*: `close()` cancels stragglers and blocks until every thread has terminated. A
thread leak is not a bug you can write.

### 7.3 Code volume

| Component | Legacy | Modern | Reduction |
|---|---:|---:|---:|
| Engine | 134 LOC | 105 LOC | 22% |
| **Nested fan-out (QC inspector)** | **66 LOC** | **35 LOC** | **47%** |

The nested case shows the bigger gap, and that is the general pattern: **structured concurrency's
advantage grows with nesting depth**, because it is precisely the composition of concurrent
operations that the old model cannot express.

---

## 8. Error handling and cancellation

### 8.1 Cancellation must be structural, not remembered

The original code had this bug, and it is worth studying:

```java
// BEFORE — a cancelled producer silently stops, and never tells anyone
catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    return;                       // consumers now wait for orders that will never come
}
```

Consumers waited on a conveyor that would never close, until the 30-second shift timeout. The fix is
structural:

```java
// AFTER — OrderProducer.call()
try {
    for (Optional<Order> next = source.next(); next.isPresent(); next = source.next()) {
        dispatch(next.get());
        ...
    }
    return null;
} finally {
    // MUST run on every exit path — normal completion, exception, or cancellation.
    conveyor.producerFinished();
}
```

That `finally` is tested explicitly:

```java
@Test
void producerReleasesConveyorOnInterrupt() throws Exception {
    ...
    future.cancel(true);              // simulate scope cancellation / shutdownNow()
    assertTrue(conveyor.isIntakeClosed(),
            "cancellation must not strand consumers on a conveyor that never closes");
}
```

**This discipline is required in both models.** Structured concurrency guarantees the *thread* dies;
it cannot guarantee your *protocol invariants* survive. That is still your job.

### 8.2 What each model gives you for free

| | Legacy | Modern |
|---|---|---|
| Cancellation propagates to children | ❌ Manual — you must hold and cancel each future | ✅ Automatic on scope exit |
| One task's failure stops its siblings | ❌ Never | ✅ With `allSuccessfulOrThrow` / `anySuccessfulOrThrow` |
| Exceptions carry a parent-child stack relationship | ❌ Lost — the future's stack is unrelated to the submitter | ✅ Preserved |
| Forgetting to check a result | 🔥 Silent — an unchecked `Future` swallows its exception forever | ✅ Surfaced by the joiner policy |

The last row deserves emphasis. `pool.submit(task)` returning a `Future` nobody calls `get()` on is
one of the most effective ways to lose a production exception permanently. There is no warning, no
log line, nothing.

### 8.3 Choosing a joiner policy

`Joiner` encodes *"when is this group of tasks done?"* as a value:

| Policy | Semantics | Use when |
|---|---|---|
| `awaitAll()` | Wait for everyone; individual failures do not cancel the group | **Long-lived peers.** Used by our engine: one picker failing shouldn't end a shift the others can finish |
| `awaitAllSuccessfulOrThrow()` | Wait for everyone; any failure cancels the rest and throws | Batch where partial success is worthless |
| `allSuccessfulOrThrow()` | As above, and `join()` returns `List<T>` in fork order | **Fan-out where you need every result.** Used by `StructuredQualityInspector` |
| `anySuccessfulOrThrow()` | First success wins; losers cancelled immediately | **Hedged requests / replica racing** |
| `allUntil(predicate)` | Custom stopping condition | Quorum ("first 3 of 5"), early exit |

In the legacy model each of these is bespoke code involving `CountDownLatch`, `AtomicReference`, and
careful cancellation. `JoinerApiDemo` in this repo shows two of them in ~20 lines total.

---

## 9. Nested parallelism and thread-starvation deadlock

This is the strongest practical argument, and it has nothing to do with performance.

### 9.1 The trap

Each picker runs two quality checks in parallel. The obvious implementation reuses the worker pool:

```java
// ☠️ DEADLOCK — do not do this
ExecutorService pool = Executors.newFixedThreadPool(8);   // 8 pickers
// ... each picker, running ON this pool, does:
pool.invokeAll(List.of(weightCheck, labelScan));          // submits BACK to the same pool
```

With 8 pickers occupying all 8 threads, every one blocks inside `invokeAll` waiting for check tasks
that are queued behind the pickers holding the threads. **Nothing can ever make progress.** The pool
is 100% "busy", CPU is 0%, and no exception is thrown — the application simply stops.

This is *thread-starvation deadlock*, and it is one of the most common ways hand-rolled nested
parallelism fails in production. It is also load-dependent: it appears only when enough pickers are
concurrently inspecting, so it passes tests and fails at peak.

### 9.2 The legacy mitigation, and what it costs

`ExecutorQualityInspector` uses a **dedicated second pool**:

```java
public ExecutorQualityInspector(int concurrentPickers) {
    this.parallelism = concurrentPickers * CHECKS_PER_ORDER;   // sizing math you must get right
    this.checkPool = Executors.newFixedThreadPool(parallelism, ...);
}

@Override
public void close() {                    // someone must remember to call this
    checkPool.shutdown();
    try {
        if (!checkPool.awaitTermination(5, TimeUnit.SECONDS)) {
            checkPool.shutdownNow();
        }
    } catch (InterruptedException e) {
        checkPool.shutdownNow();
        Thread.currentThread().interrupt();
    }
}
```

Correct — and the cost is visible in the numbers: **those 1,200 QC threads are 57% of the legacy
engine's 2,106 OS threads.** Plus a lifecycle to own, a size to tune, and a shutdown to get right.

### 9.3 The structured version

```java
public QualityReport inspect(Order order) throws InterruptedException {
    try (var scope = StructuredTaskScope.open(
            Joiner.<Boolean>allSuccessfulOrThrow(),
            cf -> cf.withName("QC-Order-" + order.orderId()))) {

        scope.fork(() -> { Thread.sleep(weightCheck(order)); return true; });
        scope.fork(() -> { Thread.sleep(labelScan(order));   return true; });

        List<Boolean> outcomes = scope.join();
        return new QualityReport(order.orderId(), outcomes.get(0), outcomes.get(1));

    } catch (StructuredTaskScope.FailedException e) {
        return new QualityReport(order.orderId(), false, false);
    }
}
```

**35 LOC instead of 66.** No pool. No sizing. No `close()`. And critically: **the deadlock class does
not exist**, because nesting scopes never competes for a fixed thread budget. Each nested fork
creates a new virtual thread; there is no queue to be stuck behind.

> If you take one thing from this document: structured concurrency does not merely make nested
> parallelism faster — **it deletes an entire category of production incident.**

---

## 10. Timeouts

**Legacy** — a supervisor loop, hand-written in both simulators of the original code:

```java
while (stats.produced() < ORDER_QUOTA) {
    if (Duration.ofNanos(System.nanoTime() - shiftStart).compareTo(SHIFT_TIMEOUT) > 0) {
        System.out.println("  [legacy] Shift timeout — manual shutdown required");
        break;
    }
    Thread.sleep(50);            // busy-poll, and 50 ms of imprecision
}
intakeOpen.set(false);           // racy: producers may be mid-put() right now
```

This polls, it is imprecise by up to 50 ms, it burns a thread, and — as the refactor found — the
`intakeOpen` flip was **racy**: it could close intake while a producer was still mid-`put()`,
stranding an order.

**Modern** — a scope configuration value:

```java
cf -> cf.withName("PeakHourShift").withTimeout(config.shiftTimeout())
```

On expiry the scope cancels every outstanding subtask and `join()` throws `TimeoutException`. No
polling thread, no imprecision, no race.

> The refactor removed the supervisor loop from **both** engines. Termination is now derived from the
> pipeline itself (`OrderSource` runs dry → last producer closes `OrderConveyor` → consumers drain and
> exit), so the timeout is a genuine *safety net* rather than the primary control mechanism. That is
> the right design in either concurrency model — but structured concurrency makes it the easy one.

---

## 11. Observability and debugging

| | Legacy | Modern |
|---|---|---|
| Thread dump size @ 900 workers | 2,106 stacks | 27 carrier stacks + 900 virtual (separate section) |
| Parent/child relationship visible | ❌ No — a pool thread's stack shows the pool, not the submitter | ✅ Yes — scope hierarchy is recorded |
| Named grouping | Thread names only | Named scopes (`PeakHourShift`, `QC-Order-4711`) |
| Exception stack traces | Truncated at the pool boundary | Include the forking frame |

The modern engine names both the scope and the threads deliberately:

```java
cf -> cf.withName("PeakHourShift")
        .withThreadFactory(Thread.ofVirtual().name("modern-vt-", 0).factory())
```

which makes the whole worker tree legible in a structured thread dump:

```bash
jcmd <pid> Thread.dump_to_file -format=json /tmp/dump.json
```

The JSON output contains a **`threadContainers` tree** mirroring your scope structure — you can see
that `QC-Order-4711` has two children and which picker forked it. In a pool-based application this
information does not exist anywhere; the relationship between a submitting thread and its executing
task is not recorded at all.

**Profiler note.** Virtual threads are supported by JFR (`jdk.VirtualThreadStart`,
`jdk.VirtualThreadPinned`, `jdk.VirtualThreadSubmitFailed`). `jdk.VirtualThreadPinned` in particular
is how you find the pinning problems described in §12.

---

## 12. When *not* to use virtual threads

Intellectual honesty matters more than advocacy here. Virtual threads are **not** a general speedup.

### 12.1 CPU-bound work — no benefit, slight loss

Virtual threads help by *unmounting on block*. Code that never blocks never unmounts, so you get all
the overhead and none of the benefit.

- ✅ Use a **fixed platform-thread pool sized to core count**, or **parallel streams** /
  `ForkJoinPool`.
- Rule of thumb: if the task does not perform I/O, do not put it on a virtual thread.

### 12.2 Pooling the *scarce* resource, not the thread

Virtual threads remove the reason to pool *threads*. They do not remove the reason to pool
**connections, file handles, or licences**.

```java
// ❌ 10,000 virtual threads will happily open 10,000 DB connections and destroy your database
try (var scope = StructuredTaskScope.open(Joiner.<Void>awaitAll())) {
    requests.forEach(r -> scope.fork(() -> db.query(r)));
}

// ✅ Limit the scarce resource explicitly — and for the right reason
Semaphore dbPermits = new Semaphore(50);         // sized to the DATABASE's capacity
try (var scope = StructuredTaskScope.open(Joiner.<Void>awaitAll())) {
    requests.forEach(r -> scope.fork(() -> {
        dbPermits.acquire();
        try { return db.query(r); } finally { dbPermits.release(); }
    }));
}
```

The improvement is conceptual: the limit now expresses *"the database supports 50 concurrent
queries"* rather than *"our thread pool happens to have 50 threads"*. Same number, honest reason.

> `OrderConveyor` does the analogous thing with a bounded queue and timed `offer()` — back-pressure
> is deliberate and observable, not an accident of pool sizing.

### 12.3 `ThreadLocal` at scale

`ThreadLocal` costs are per-thread. At 1,000,000 virtual threads, a 1 KB thread-local becomes 1 GB.

- Audit libraries for `ThreadLocal` caches (many JSON/XML parsers and date formatters have them).
- Prefer **`ScopedValue`** (JDK 25+) for context propagation — immutable, inherited by forked
  subtasks, and released at scope exit.

### 12.4 Pinning

A virtual thread that cannot unmount **pins** its carrier, degrading it to a platform thread.

| Cause | Status on JDK 26 |
|---|---|
| `synchronized` block containing blocking I/O | ✅ **Fixed in JDK 24 (JEP 491)** — no longer pins |
| Native frames (JNI) on the stack | ⚠️ **Still pins** |
| Class initialisers that block | ⚠️ **Still pins** |

Because this project targets JDK 26, the `synchronized` problem — historically *the* reason virtual
thread migrations failed — is gone. Use `ReentrantLock` where you need `tryLock`/fairness, not
because of pinning. Monitor `jdk.VirtualThreadPinned` JFR events for the remaining causes.

### 12.5 Ordering and fairness

The virtual thread scheduler is a `ForkJoinPool` in FIFO mode and offers **no fairness or priority
guarantees**. `Thread.setPriority` is ignored. If you need strict prioritisation, schedule it
explicitly — do not rely on the thread system.

---

## 13. Reproducing these measurements

Everything in this document comes from two benchmark classes in `com.java.learning.bench`.

```bash
mvn -q -DskipTests compile
```

**Raw thread cost** (`ThreadFootprintBenchmark <platform|virtual> <count>`):

```bash
/usr/bin/time -l java --enable-preview -cp target/classes \
    com.java.learning.bench.ThreadFootprintBenchmark platform 5000

/usr/bin/time -l java --enable-preview -Xmx4g -cp target/classes \
    com.java.learning.bench.ThreadFootprintBenchmark virtual 1000000
```

**Find your machine's ceiling** — ask for more than it can give:

```bash
java --enable-preview -cp target/classes \
    com.java.learning.bench.ThreadFootprintBenchmark platform 20000
```

**Engine footprint** (`EngineFootprintBenchmark <modern|legacy> [producers] [consumers] [orders]`):

```bash
for e in modern legacy; do
  /usr/bin/time -l java --enable-preview -cp target/classes \
      com.java.learning.bench.EngineFootprintBenchmark $e 300 600 2000
done

# Where legacy breaks and modern does not
for e in modern legacy; do
  /usr/bin/time -l java --enable-preview -cp target/classes \
      com.java.learning.bench.EngineFootprintBenchmark $e 500 2000 3000
done
```

On Linux use `/usr/bin/time -v` and read *Maximum resident set size*. Your ceiling will differ —
check `ulimit -u` and `/proc/sys/kernel/threads-max`.

**The simulation itself**, with its own comparison table:

```bash
mvn -q compile exec:exec -Dsim.args="all --orders=2000 --producers=300 --consumers=600 --buffer=256"
```

---

## 14. Decision matrix and migration checklist

### 14.1 Which model for which workload

| Workload | Recommendation | Why |
|---|---|---|
| Blocking I/O, high concurrency (HTTP, DB, RPC) | ✅ **Virtual threads** | The design centre. 48× memory, ~3× CPU, no ceiling |
| Request-per-thread web service | ✅ **Virtual threads** | Removes the pool cap that forces reactive rewrites |
| Fan-out to several services, all results needed | ✅ **`allSuccessfulOrThrow`** | Auto-cancels siblings on first failure |
| Hedged/replica requests, fastest wins | ✅ **`anySuccessfulOrThrow`** | Cancels losers automatically |
| Long-lived peer workers (our warehouse shift) | ✅ **`awaitAll`** | One worker failing shouldn't end the shift |
| CPU-bound compute | ❌ **Platform pool sized to cores** | Never blocks, so never benefits |
| Rate-limiting a scarce downstream | ⚠️ **Virtual threads + `Semaphore`** | Limit the resource, not the thread |
| Existing reactive code that works | 🤷 **Leave it** | Migrate for maintainability, not for speed |

### 14.2 Migration checklist

1. **Measure first.** If thread count is not your constraint, the win is maintainability, not
   throughput. Say so honestly.
2. **Never pool virtual threads.** `Executors.newVirtualThreadPerTaskExecutor()`, or fork into a
   scope. Pooling them reintroduces every problem they solve.
3. **Audit `ThreadLocal`.** Replace context propagation with `ScopedValue`.
4. **Limit scarce resources with a `Semaphore`**, sized to the downstream's real capacity.
5. **Keep CPU-bound work on platform threads.**
6. **Enable JFR** and watch `jdk.VirtualThreadPinned`.
7. **Put cleanup in `finally`.** Structured concurrency guarantees the thread dies; *your protocol
   invariants are still your job* (see §8.1).
8. **Choose the joiner deliberately.** `awaitAll` vs `allSuccessfulOrThrow` is a real semantic
   decision about whether one failure should end the group.
9. **Name your scopes.** Free observability; costs one method call.
10. **`StructuredTaskScope` is preview in JDK 26** — `--enable-preview` at *both* compile and run
    time. (Note that only preview-*using* classes are marked, which is why `exec:java` fails and this
    project uses `exec:exec` to fork a properly flagged JVM.)

---

## 15. Glossary

| Term | Meaning |
|---|---|
| **Carrier thread** | The platform thread a virtual thread runs on while mounted. Default pool = one per core |
| **Mount / unmount** | Attaching/detaching a virtual thread to/from a carrier. User-mode, no syscall |
| **Continuation** | Heap object holding a virtual thread's stack while unmounted |
| **Pinning** | A virtual thread that cannot unmount, holding its carrier hostage |
| **Structured concurrency** | Concurrent tasks' lifetimes are confined to a lexical scope, like local variables |
| **Scope** | `StructuredTaskScope` — owns forked subtasks; closing cancels and joins all of them |
| **Joiner** | Policy deciding when a scope is done and what `join()` returns |
| **Subtask** | Handle to a forked task; exposes `state()`, `get()`, `exception()` |
| **Thread-starvation deadlock** | Pool tasks block on other tasks queued behind them in the same pool |
| **IPC** | Instructions per cycle. Low IPC = stalls (cache misses, TLB pressure) |
| **RSS** | Resident set size — physical memory actually held. Includes native thread stacks; heap metrics do not |
| **Little's Law** | `concurrency = arrival_rate × latency`. Determines required pool size |
| **`ScopedValue`** | Immutable, scope-confined replacement for `ThreadLocal` (JDK 25+) |

---

## Appendix: complete raw results

### A.1 Thread footprint probe

| Kind | Count | Created | Create time | µs/thread | Heap/thread | OS threads | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| platform | 10 | 10 | — | — | — | 16 | 51.7 MB |
| platform | 1,000 | 1,000 | 39 ms | 39.1 | 297 B | 1,006 | 135.9 MB |
| platform | 2,000 | 2,000 | 84 ms | 42.2 | 294 B | 2,006 | 177.9 MB |
| platform | 5,000 | 5,000 | 433 ms | 86.6 | — | 5,006 | 314.3 MB |
| platform | 20,000 | **6,116** 💥 | 575 ms | 93.9 | — | 6,122 | 348.4 MB |
| virtual | 10 | 10 | 4 ms | 447.3 | 6,116 B | 10 | 48.6 MB |
| virtual | 1,000 | 1,000 | 15 ms | 14.6 | 2,584 B | 25 | 59.4 MB |
| virtual | 2,000 | 2,000 | 17 ms | 8.7 | 2,330 B | 25 | 64.3 MB |
| virtual | 5,000 | 5,000 | 23 ms | 4.6 | 2,043 B | 25 | 75.0 MB |
| virtual | 10,000 | 10,000 | 36 ms | 3.6 | 1,730 B | 25 | 90.1 MB |
| virtual | 100,000 | 100,000 | 101 ms | 1.0 | 886 B | 25 | 211.1 MB |
| virtual | 1,000,000 | 1,000,000 | 493 ms | 0.5 | 822 B | **25** | 1,149 MB |

### A.2 Engine footprint

| Engine | Workers | Orders | Elapsed | Throughput | Peak OS threads | Peak RSS | Instructions | Cycles | IPC | Outcome |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| modern | 900 | 2,000 | 388 ms | 5,155/s | **27** | 95.3 MB | 3.665 B | 3.222 B | **1.14** | CLEAN |
| legacy | 900 | 2,000 | 426 ms | 4,695/s | 2,106 | 254.9 MB | 3.523 B | 9.702 B | 0.36 | CLEAN |
| modern | 2,500 | 3,000 | 244 ms | 12,295/s | **27** | 115.5 MB | — | 4.0 B | — | CLEAN |
| legacy | 2,500 | — | — | — | — | 553.8 MB | — | 52.4 B | — | 💥 **OOM: native thread** |
| modern | 12,000 | 20,000 | 393 ms | 50,891/s | **27** | 244.5 MB | — | 16.2 B | — | CLEAN |
| legacy | 12,000 | — | — | — | — | — | — | — | — | 💥 not expressible |

---

## References

- JEP 444 — Virtual Threads (final, JDK 21)
- JEP 491 — Synchronize Virtual Threads without Pinning (JDK 24)
- JEP 505 — Structured Concurrency (preview lineage through JDK 25/26)
- JEP 506 — Scoped Values (final, JDK 25)
- *Java Concurrency in Practice*, Goetz et al. — still the reference for the legacy model's hazards
- `java.util.concurrent.StructuredTaskScope` javadoc (JDK 26)







