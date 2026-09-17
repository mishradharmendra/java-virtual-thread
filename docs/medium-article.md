# Same Work, 3× the CPU

### The memory savings get all the attention. The number that actually changed my mind was instructions-per-cycle.

---

I went into this expecting the usual virtual threads story: less memory, more threads, everyone
claps. I got that. I also got a number I wasn't looking for, and it's the one I've been repeating to
people ever since.

Two versions of the same program. Same workload, same business logic — literally the same objects,
I'll show you how — one running on a fixed pool of platform threads, one on virtual threads inside a
`StructuredTaskScope`.

Both retire about 3.5 billion CPU instructions. Identical work, and the instruction counts prove it.

The thread pool version needs **9.7 billion cycles** to get through them. The virtual thread version
needs **3.2 billion**.

Same instructions. Three times the CPU. That's the part nobody put in a slide deck for me.

---

## What I was actually building

I'd been writing a toy warehouse simulator to get familiar with JDK 26. Orders come in from five
sales channels, land on a conveyor belt (a bounded queue), and pickers take them off, "walk the
aisle" for a while, run two quality checks in parallel, and ship. Classic producer/consumer with a
bit of nesting to make it interesting.

I'd written it twice — once with `ExecutorService`, once with structured concurrency — so I could
compare them.

Then I actually read what I'd written, and the comparison fell apart before I'd measured anything.

The two versions didn't do the same work. The modern one ran quality checks; the legacy one didn't.
The constants were copy-pasted into both files and had already drifted. Both versions had the
producer and consumer logic inlined as anonymous lambdas, tangled up with pool management, quota
supervision, and `System.out.println`. Around 200 lines each, doing about six jobs.

You can't benchmark that. You can only benchmark two different programs and pretend.

So before any measurement, I pulled the producers and consumers out into real classes.

---

## Pulling it apart found four bugs

This is the part I didn't expect, and honestly it's the more useful half of this article. The
refactoring wasn't cosmetic. Extracting the loops into named types with single responsibilities made
four genuine concurrency bugs obvious, and they'd been sitting there the whole time passing my
eyeball test.

### Bug 1: the quota was never actually a quota

Every producer ran this:

```java
while (intakeOpen.get() && stats.produced() < ORDER_QUOTA) {
    Order order = new Order(orderSequence.incrementAndGet(), channel, ...);
    conveyor.put(order);
    stats.recordProduced();
}
```

Read it as one producer and it's fine. Read it as five producers and it's a check-then-act race.
All five can evaluate `stats.produced() < ORDER_QUOTA` at 99 orders, all five pass, all five
produce. You asked for 100 and got 104.

I'd written this. I've reviewed code like this. It reads as obviously correct right up until you
count the threads.

The fix is to make claiming a slot and incrementing the counter the same atomic operation:

```java
public Optional<Order> next() {
    long ticket;
    do {
        ticket = issued.get();
        if (ticket >= quota) {
            return Optional.empty();
        }
        // CAS is what makes the bound exact: only the winner of this slot proceeds.
    } while (!issued.compareAndSet(ticket, ticket + 1));

    long orderId = ticket + 1;
    return Optional.of(new Order(orderId, SalesChannel.forSequence(ticket), ...));
}
```

Now a producer's whole loop is "drain this until it's empty." No shared flag, no supervisor thread
watching a counter, no polling. The termination condition *is* the quota.

I wrote a test that runs 64 threads against one source and asserts the count is exactly right. It
passes now. It would not have passed before.

### Bug 2: consumers could quit while an order was still in the air

The consumers' exit condition looked like this:

```java
Order order = conveyor.poll(200, TimeUnit.MILLISECONDS);
if (order == null) {
    if (!intakeOpen.get() && conveyor.isEmpty()) {
        break;
    }
    continue;
}
```

And somewhere else, a supervisor thread flipped `intakeOpen` to false when the quota was hit.

Spot it? There's a window. The supervisor can flip the flag while a producer is sitting inside
`conveyor.put()` with an order it already claimed. A consumer wakes up, sees intake closed and the
queue empty, and leaves. The producer's order lands on a conveyor nobody is watching.

This is the kind of bug that shows up as "we're missing about one order in every ten thousand runs"
six months later.

What fixed it wasn't a cleverer check. It was moving the termination protocol into one place and
giving it an invariant I could state in a sentence:

> Intake closes **only** when the last registered producer has finished. A producer signals that
> strictly after its final `offer()` returns. So once `intakeClosed` is true, no further order can
> ever arrive, and `intakeClosed && queue.isEmpty()` genuinely means *drained*.

```java
public void producerFinished() {
    if (activeProducers.decrementAndGet() == 0) {
        intakeClosed = true;
    }
}

public Optional<Order> take() throws InterruptedException {
    while (true) {
        Order order = queue.poll(drainPollInterval.toNanos(), TimeUnit.NANOSECONDS);
        if (order != null) {
            return Optional.of(order);
        }
        if (intakeClosed && queue.isEmpty()) {
            return Optional.empty();
        }
    }
}
```

I considered poison pills and rejected them. On a bounded queue, pushing sentinels at shutdown can
itself block when the buffer is full — you'd reintroduce the deadlock you were trying to remove.
Counting producers out is less clever and more correct.

### Bug 3: the missing `finally`

This one's small and vicious:

```java
catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    return;
}
```

A cancelled producer stops. It doesn't tell the conveyor it's gone. So the "last producer closes
intake" rule never fires, and every consumer blocks until the 30-second shift timeout.

```java
@Override
public Void call() throws InterruptedException {
    tagCurrentThread();
    try {
        for (Optional<Order> next = source.next(); next.isPresent(); next = source.next()) {
            dispatch(next.get());
            if (!intakeDelay.isZero()) {
                Thread.sleep(intakeDelay);
            }
        }
        return null;
    } finally {
        // MUST run on every exit path — normal completion, exception, or cancellation.
        conveyor.producerFinished();
    }
}
```

Worth saying clearly, because it cuts against the marketing: **structured concurrency does not save
you here.** It guarantees the thread dies. It cannot guarantee your protocol invariants survive the
thread dying. That `finally` is still your job in both models.

I wrote a test that cancels a producer mid-flight and asserts the conveyor closed anyway. It's one
of my favourite tests in the project because it tests a thing that is invisible in the happy path.

### Bug 4: `put()` with no escape hatch

`conveyor.put(order)` blocks forever if the queue is full. If every consumer dies, every producer
parks permanently and only the shift timeout breaks it.

I swapped it for a timed `offer()` and counted the failures:

```java
private void dispatch(Order order) throws InterruptedException {
    if (conveyor.offer(order)) {
        stats.recordProduced();
    } else {
        // Conveyor stayed full past the offer timeout: shed the order rather than block
        // forever. Visible load shedding beats an invisible deadlock.
        stats.recordRejected();
    }
}
```

Funny detail: `recordRejected()` already existed in the stats class. It had never been called by
anything. Dead code sitting there since the first commit, waiting for someone to notice the thing it
was obviously meant to handle.

---

## Making the comparison honest

Here's the bit that made the benchmark worth running.

Producers and consumers became plain `Callable<Void>` implementations that know nothing about how
they're scheduled:

```java
public sealed interface WarehouseWorker extends Callable<Void>
        permits OrderProducer, OrderConsumer {
    String name();
    WorkerRole role();
}
```

One assembler builds the roster. Both engines take the same roster. The legacy engine hands those
exact instances to `pool.submit()`; the modern one forks them into a scope.

Not "equivalent logic." The same objects.

The quality checks needed a seam too, since the modern version had them and the legacy one didn't.
That became a `QualityInspector` strategy with three implementations — sequential, executor-based,
and structured. Both engines now run identical nested work.

Once that was done I could finally measure something meaningful.

---

## What a thread costs

First question: what does a thread actually cost? I wrote a probe that creates N threads, parks all
of them on a latch (the state a blocked worker is in), and reports.

Important detail that took me a minute to appreciate: **you have to measure RSS from outside the
JVM.** A platform thread's stack is native memory. It never shows up in heap statistics. If you
measure only the heap you'll conclude platform threads are cheaper, which is exactly backwards.

So: `/usr/bin/time -l`, one JVM per measurement.

Machine: 18-core Apple Silicon, macOS, Amazon Corretto 26.0.2.1. Baseline JVM ≈ 50 MB.

| Threads | Platform RSS | Platform OS threads | Virtual RSS | Virtual OS threads |
|---:|---:|---:|---:|---:|
| 1,000 | 135.9 MB | 1,006 | **59.4 MB** | **25** |
| 2,000 | 177.9 MB | 2,006 | **64.3 MB** | **25** |
| 5,000 | 314.3 MB | 5,006 | **75.0 MB** | **25** |
| 10,000 | 💥 | — | **90.1 MB** | **25** |
| 100,000 | 💥 | — | **211.1 MB** | **25** |
| 1,000,000 | 💥 | — | **1,149 MB** | **25** |

Marginal cost per thread, from the slope: **53.8 KB** for platform, **1.13 KB** for virtual. About
48× cheaper.

A million virtual threads took 493 ms to create and park. Still 25 OS threads.

The creation cost trend is the part I keep coming back to. Platform threads get *more* expensive per
thread as you add them — 39 µs each at 1,000, 87 µs each at 5,000. Virtual threads get *cheaper* —
14.6 µs at 1,000, 0.5 µs at a million, as JIT warms up and fixed costs amortise.

Which reframes what thread pools are for. Pooling exists because creation is expensive. When creation
costs half a microsecond, pooling stops being a performance technique. And once you stop pooling, you
also stop having pool sizing, pool starvation, and thread-locals leaking between unrelated tasks.

## The wall

I asked for 20,000 platform threads to see what would happen.

```
created             : 6,116  <-- HIT THE CEILING
failure             : java.lang.OutOfMemoryError: unable to create native thread:
                      possibly out of memory or process/resource limits reached
```

6,116. Not a heap limit — `kern.num_taskthreads` on this box is 6,144 and the JVM already held 28.
No `-Xmx` value helps. It's an OS resource limit that arrives dressed as an `OutOfMemoryError`,
which is a genuinely unhelpful piece of API design.

That ceiling is lower than most people assume. It's in the same range as "concurrent connections a
moderately busy service handles."

---

## Now the actual benchmark

900 workers — 300 producers, 600 consumers — fulfilling 2,000 orders. Both engines complete all
2,000. Single paired run so the CPU counters are comparable.

| | Legacy | Modern |
|---|---:|---:|
| Wall clock | 426 ms | **388 ms** |
| Throughput | 4,695/s | **5,155/s** |
| **Peak OS threads** | **2,106** | **27** |
| **Peak RSS** | 254.9 MB | **95.3 MB** |
| Instructions retired | 3.523 B | 3.665 B |
| **CPU cycles** | **9.702 B** | **3.222 B** |
| **IPC** | **0.36** | **1.14** |
| Involuntary context switches | 39,739 | 25,699 |

Wall clock is a 10% difference. On its own, not a headline — and if that were the whole story I'd
have shrugged and moved on.

The OS thread count is the obvious one. 2,106 versus 27. Where does 2,106 come from?

```
  900  worker threads      (fixed pool: producers + consumers)
+1200  QC check threads    (600 consumers × 2 parallel checks)
+   6  JVM internals
──────
 2,106
```

In the pool model, "how much concurrency do I have" and "how many OS threads do I have" are the same
number. They're the same number by construction. Virtual threads break that equivalence, and almost
everything else follows from it.

But the row I stared at longest is the CPU one.

**The legacy engine retires slightly fewer instructions — 3.523 B vs 3.665 B — and needs three times
as many cycles to get through them.** IPC of 0.36 against 1.14.

That gap isn't work. It's stalls. 2,106 live thread stacks each touching their own pages, evicting
each other out of L1 and L2, blowing out the TLB. Plus every block and unblock crossing into the
kernel, where a virtual thread's unmount is a user-mode continuation switch with no syscall at all.
27 carrier stacks stay hot in cache. 2,106 do not.

Translated: on anything billed per vCPU, the pool model costs roughly 3× the CPU for the same
business outcome on blocking workloads. And it's invisible in application metrics — throughput looks
similar, latency looks similar, you just quietly need more machines.

There's also a trap in the memory numbers worth calling out. The legacy run reports a *smaller Java
heap* — 9.2 MB against 20.5 MB — while using 2.7× more actual memory. Virtual thread stacks live on
the heap and are visible. Platform thread stacks are native and are not. If your dashboards only
graph heap, this comparison will tell you the wrong thing.

## Then I pushed it past the ceiling

2,500 workers, which needs 2,500 worker threads plus 4,000 QC threads. That's 6,500. The ceiling is
6,116.

| | Legacy | Modern |
|---|---|---|
| Result | 💥 `OutOfMemoryError: unable to create native thread` | ✅ 244 ms, 12,295 orders/s |
| Peak OS threads | — | **27** |
| CPU cycles | **52.4 B** (burned while failing) | **4.0 B** |

The legacy engine spent **thirteen times more CPU failing than the modern engine spent succeeding.**
Thread creation costs whether or not it works out.

At 12,000 workers — double the platform ceiling — the modern engine did 20,000 orders in 393 ms at
50,891 orders/s. Twenty-seven OS threads. Still.

---

## The argument that has nothing to do with performance

If you only take one thing from this, take this section. It's the one I'd actually lead with in a
design review.

Each picker runs two quality checks in parallel. The obvious implementation reuses the worker pool:

```java
// ☠️ this deadlocks
ExecutorService pool = Executors.newFixedThreadPool(8);   // 8 pickers
// each picker, running ON this pool, does:
pool.invokeAll(List.of(weightCheck, labelScan));          // submits BACK to the same pool
```

Eight pickers occupy all eight threads. Every one blocks inside `invokeAll` waiting for check tasks
queued behind the pickers holding the threads. Nothing progresses. Ever.

The pool reports 100% busy. CPU sits at zero. No exception, no log line, no stack trace pointing at
anything. The application just stops.

Thread-starvation deadlock. And it's load-dependent — it only appears when enough pickers happen to
be inspecting simultaneously, so it passes every test you wrote and fails at 2am on Black Friday.

The standard fix is a second, dedicated pool:

```java
public ExecutorQualityInspector(int concurrentPickers) {
    this.parallelism = concurrentPickers * CHECKS_PER_ORDER;  // sizing math you must get right
    this.checkPool = Executors.newFixedThreadPool(parallelism, ...);
}

@Override
public void close() {                    // and someone has to remember to call this
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

That works. It also accounts for 1,200 of the legacy engine's 2,106 OS threads — 57% of its entire
thread footprint exists to avoid a deadlock.

Here's the structured version:

```java
public QualityReport inspect(Order order) throws InterruptedException {
    try (var scope = StructuredTaskScope.open(
            Joiner.<Boolean>allSuccessfulOrThrow(),
            cf -> cf.withName("QC-Order-" + order.orderId()))) {

        scope.fork(() -> {
            Thread.sleep(QualityCheckProfile.weightCheck(order));
            return true;
        });
        scope.fork(() -> {
            Thread.sleep(QualityCheckProfile.labelScan(order));
            return true;
        });

        // JDK 26 returns the results directly, in fork order.
        List<Boolean> outcomes = scope.join();
        return new QualityReport(
                order.orderId(),
                Boolean.TRUE.equals(outcomes.get(WEIGHT_CHECK)),
                Boolean.TRUE.equals(outcomes.get(LABEL_SCAN)));

    } catch (StructuredTaskScope.FailedException e) {
        // One check blew up; the scope already cancelled its sibling on the way out.
        return new QualityReport(order.orderId(), false, false);
    }
}
```

35 lines against 66. No pool. Nothing to size. Nothing to close. And critically: **the deadlock
cannot happen**, because nesting scopes never competes for a fixed thread budget. There's no queue
to be stuck behind.

That `join()` returning `List<Boolean>` directly is new in JDK 26, by the way. In earlier previews
you got a `Stream<Subtask<Boolean>>` and had to `.map(Subtask::get)` your way out of it.

Structured concurrency doesn't make nested parallelism faster. It deletes a category of production
incident.

---

## What the engine code looks like now

Legacy — the lifecycle, in full:

```java
ExecutorService pool = Executors.newFixedThreadPool(config.totalWorkers(), threadFactory());
try {
    for (WarehouseWorker worker : assembly.workers()) {
        submissions.add(new Submission(worker, pool.submit(worker)));
    }
    timedOut = !awaitOrderlyShutdown(pool);
} finally {
    if (!pool.isTerminated()) {
        pool.shutdownNow();
    }
}

// ...plus a dedicated method just to shut down:
private boolean awaitOrderlyShutdown(ExecutorService pool) throws InterruptedException {
    pool.shutdown();
    if (pool.awaitTermination(config.shiftTimeout().toNanos(), NANOSECONDS)) {
        return true;
    }
    reporter.note("Shift timeout exceeded — escalating to shutdownNow()");
    pool.shutdownNow();
    if (!pool.awaitTermination(FORCED_SHUTDOWN_GRACE.toNanos(), NANOSECONDS)) {
        reporter.note("Pool still alive after shutdownNow() — workers ignoring interrupts");
    }
    return false;
}

// ...plus a loop to drain every Future individually, because nothing links them together.
```

Modern:

```java
try (var scope = StructuredTaskScope.open(
        Joiner.<Void>awaitAll(),
        cf -> cf.withName("PeakHourShift")
                .withThreadFactory(virtualThreadFactory())
                .withTimeout(config.shiftTimeout()))) {

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
// Closing brace: every virtual thread has terminated. Guaranteed.
```

The timeout went from a hand-written supervisor loop polling a counter every 50 ms — imprecise, and
racy in the original, since it flipped a shared flag while producers were mid-write — to
`.withTimeout(...)` in the scope config.

But the thing I care about is that closing brace. In the pool version, "no thread outlives this
method" is enforced by a `finally` block. By someone remembering. In the scope version it's enforced
by the language: `close()` cancels stragglers and blocks until every thread has terminated.

A thread leak isn't a bug you can write. That's a different category of guarantee than "we have a
code review checklist."

134 lines to 105 for the engine. 66 to 35 for the nested case. The nested gap is bigger, and that's
the pattern — structured concurrency's advantage scales with nesting depth, because composition is
exactly what the old model can't express.

---

## Where I'd still reach for a thread pool

I don't want this to read as a sales pitch, so:

**CPU-bound work.** Virtual threads help by unmounting when you block. Code that never blocks never
unmounts. You get the overhead and none of the benefit. Fixed pool sized to your core count, or
parallel streams. Rule of thumb: no I/O, no virtual thread.

**Pooling the scarce resource.** Virtual threads remove the reason to pool *threads*. They don't
remove the reason to limit database connections. Ten thousand virtual threads will cheerfully open
ten thousand connections and take your database down.

```java
Semaphore dbPermits = new Semaphore(50);   // sized to the DATABASE's capacity
try (var scope = StructuredTaskScope.open(Joiner.<Void>awaitAll())) {
    requests.forEach(r -> scope.fork(() -> {
        dbPermits.acquire();
        try { return db.query(r); } finally { dbPermits.release(); }
    }));
}
```

Same number as your old pool size, probably. But now it means "the database supports 50 concurrent
queries" instead of "our thread pool happens to have 50 threads." Honest reason, same limit.

**`ThreadLocal` at scale.** Costs are per-thread. A 1 KB thread-local across a million virtual
threads is a gigabyte. Audit your libraries — plenty of parsers and formatters cache in thread-locals
— and use `ScopedValue` for context propagation.

**Pinning.** A virtual thread that can't unmount pins its carrier and degrades it to a platform
thread. The big one — `synchronized` blocks containing blocking I/O — was fixed in JDK 24 by JEP 491,
which quietly removed the main reason virtual thread migrations used to fail. JNI frames and blocking
class initialisers still pin. Watch `jdk.VirtualThreadPinned` in JFR.

**Working reactive code.** If you've got a Reactor or RxJava codebase that works, migrating buys you
readability and debuggability, not throughput. That's a real reason. It's just not a performance
reason, and you should say so out loud when you ask for the time.

---

## A few things that cost me time

Because articles like this usually skip the friction, and the friction is half the experience.

`Joiner.anySuccessfulResultOrThrow()` doesn't exist. It's `anySuccessfulOrThrow()`. I'd carried the
wrong name over from an older preview. `javap` on `java.util.concurrent.StructuredTaskScope$Joiner`
sorted it out in about fifteen seconds, which is a good reminder that the JDK ships its own
documentation.

`exec:java` can't run this project. When you compile with `--enable-preview`, javac marks only the
class files that actually *use* preview APIs — `ModernWarehouseEngine` gets class-file minor version
65535, `Main` gets 0. The JVM refuses to load a marked class unless it was itself started with
`--enable-preview`, and Maven's JVM wasn't. So `exec:java`, which runs in-process, dies. You need
`exec:exec` to fork a properly flagged JVM. That's a fun twenty minutes.

And I spent longer than I'd like to admit on a Maven build failure before remembering that XML
comments can't contain a double hyphen. I'd written `--enable-preview` inside a `<!-- -->` block.
Parser was right, I was wrong.

One more: my first test run had a single failure, on an assertion about channel round-robin for
`Long.MIN_VALUE`. I'd asserted the wrong expected value. The code was fine; I'd done the modular
arithmetic in my head and got it backwards. Worth mentioning only because the instinct when a test
fails is to suspect the code, and roughly half the time it's the test.

---

## So what do I actually think

If you're picking a concurrency model for new blocking-I/O code on JDK 21 or later, use virtual
threads. That's not a close call anymore, and JEP 491 removed the last common reason to hesitate.

But I'd argue for it differently than I would have a month ago.

The memory numbers are real and they're large — 48× per thread — and they're still the least
interesting thing here. Most services aren't memory-bound on thread stacks. They're bound by a pool
size someone picked in 2019, and by the queueing latency that pool causes when a downstream
dependency has a bad afternoon.

The CPU finding is what I'd put in front of a skeptical staff engineer. Same instructions, three
times the cycles, and it doesn't show up on any dashboard you currently have. Your throughput graph
looks fine. You're just paying for three times the compute.

The ceiling is what I'd put in front of a room that thinks this doesn't apply to them. 6,116 threads
on a well-specified laptop. Not a theoretical limit — a hard stop, reported as a misleading
`OutOfMemoryError`, that arrives well inside the range of "a service having a busy day."

And the deadlock is what I'd lead with in a design review, because it isn't about performance at all.
Nested parallelism on a shared pool is a trap that passes tests, holds up under load testing, and
then takes production down at the worst possible moment. Structured concurrency doesn't mitigate that
risk. It makes the code that causes it unwriteable.

The honest caveat: none of this rescued me from the four bugs in my own code. The quota race, the
termination race, the missing `finally`, the unbounded `put()` — all four survived the rewrite to
structured concurrency, and all four were found by *pulling producer and consumer into separate
classes with one job each* and then being able to write a test for each of them.

Virtual threads made my program cheaper to run. Giving every piece one responsibility made it
correct. Those turned out to be completely different projects, and I'd rank the second one higher.

---

*The full project — both engines, the benchmark harness, and 48 tests including the ones that catch
each bug described above — is on GitHub. Every number in this article is reproducible with two
commands; the detailed measurement writeup is in `docs/CONCURRENCY-COMPARISON.md`.*

*Measured on an 18-core Apple Silicon Mac running macOS 26.6 and Amazon Corretto 26.0.2.1. Your
thread ceiling will differ — check `ulimit -u` and `kern.num_taskthreads`, or
`/proc/sys/kernel/threads-max` on Linux.*


