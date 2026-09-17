package com.java.learning.bench;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Measures what a thread actually costs, so the legacy/modern comparison can be argued with numbers
 * instead of adjectives.
 *
 * <p>Creates {@code N} threads of the requested kind, parks every one of them on a latch (the state
 * a blocked worker is in) and reports creation time, heap delta and live OS thread count.
 *
 * <p>Run one kind per JVM so the operating system's view is clean, and wrap it in
 * {@code /usr/bin/time -l} (macOS) or {@code /usr/bin/time -v} (Linux) to capture peak RSS — the
 * number that actually matters, because a platform thread's stack is native memory the JVM heap
 * never sees.
 *
 * <pre>{@code
 * /usr/bin/time -l java --enable-preview -cp target/classes \
 *     com.java.learning.bench.ThreadFootprintBenchmark platform 10000
 * }</pre>
 */
public final class ThreadFootprintBenchmark {

    private static final int DEFAULT_COUNT = 10_000;

    private ThreadFootprintBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        Kind kind = args.length > 0 ? Kind.parse(args[0]) : Kind.VIRTUAL;
        int requested = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_COUNT;

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();

        settle();
        long heapBefore = memory.getHeapMemoryUsage().getUsed();
        int osThreadsBefore = threads.getThreadCount();

        var allStarted = new CountDownLatch(requested);
        var release = new CountDownLatch(1);
        List<Thread> started = new ArrayList<>(requested);

        long createNanos;
        Throwable creationFailure = null;
        long start = System.nanoTime();
        try {
            for (int i = 0; i < requested; i++) {
                Thread thread = kind.builder()
                        .unstarted(() -> {
                            allStarted.countDown();
                            try {
                                // Parked, exactly like a worker blocked on a queue or a socket.
                                release.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
                thread.start();
                started.add(thread);
            }
            allStarted.await(60, TimeUnit.SECONDS);
        } catch (OutOfMemoryError | InternalError e) {
            // Platform threads run out long before virtual threads do. That IS the result.
            creationFailure = e;
        } finally {
            createNanos = System.nanoTime() - start;
        }

        settle();
        long heapAfter = memory.getHeapMemoryUsage().getUsed();
        int osThreadsAfter = threads.getThreadCount();
        int live = started.size();

        report(kind, requested, live, createNanos, heapAfter - heapBefore,
                osThreadsBefore, osThreadsAfter, creationFailure);

        release.countDown();
        // Do not join: with hundreds of thousands of threads the teardown dominates the run and
        // tells us nothing. The JVM exits once the report is flushed.
        System.out.flush();
        Runtime.getRuntime().halt(0);
    }

    private static void report(Kind kind,
                               int requested,
                               int live,
                               long createNanos,
                               long heapDelta,
                               int osThreadsBefore,
                               int osThreadsAfter,
                               Throwable failure) {
        double createMillis = createNanos / 1_000_000.0d;

        System.out.printf(Locale.ROOT, "kind                : %s%n", kind.label());
        System.out.printf(Locale.ROOT, "requested           : %,d%n", requested);
        System.out.printf(Locale.ROOT, "created             : %,d%s%n", live,
                failure == null ? "" : "  <-- HIT THE CEILING");
        System.out.printf(Locale.ROOT, "create + start      : %,.0f ms  (%.1f us/thread)%n",
                createMillis, live == 0 ? 0 : createNanos / 1_000.0d / live);
        System.out.printf(Locale.ROOT, "heap delta          : %,.1f MB  (%,d bytes/thread)%n",
                heapDelta / 1_048_576.0d, live == 0 ? 0 : heapDelta / live);
        System.out.printf(Locale.ROOT, "OS threads before   : %,d%n", osThreadsBefore);
        System.out.printf(Locale.ROOT, "OS threads after    : %,d  (delta %,d)%n",
                osThreadsAfter, osThreadsAfter - osThreadsBefore);
        System.out.printf(Locale.ROOT, "available processors: %d%n",
                Runtime.getRuntime().availableProcessors());

        if (failure != null) {
            System.out.printf(Locale.ROOT, "failure             : %s: %s%n",
                    failure.getClass().getName(), failure.getMessage());
        }
    }

    /** Best-effort quiescing so the heap delta reflects threads rather than garbage. */
    private static void settle() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(120);
        }
    }

    private enum Kind {
        PLATFORM {
            @Override
            Thread.Builder builder() {
                return Thread.ofPlatform().daemon().name("probe-p-", 0);
            }

            @Override
            String label() {
                return "platform (1 OS thread each)";
            }
        },
        VIRTUAL {
            @Override
            Thread.Builder builder() {
                return Thread.ofVirtual().name("probe-v-", 0);
            }

            @Override
            String label() {
                return "virtual (M:N on carriers)";
            }
        };

        abstract Thread.Builder builder();

        abstract String label();

        static Kind parse(String token) {
            return switch (token.toLowerCase(Locale.ROOT)) {
                case "platform", "p" -> PLATFORM;
                case "virtual", "v" -> VIRTUAL;
                default -> throw new IllegalArgumentException(
                        "Unknown thread kind '" + token + "' (expected: platform | virtual)");
            };
        }
    }
}

