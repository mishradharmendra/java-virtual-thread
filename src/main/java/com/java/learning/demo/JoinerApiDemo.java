package com.java.learning.demo;

import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;

/**
 * Standalone showcase of the JDK 26 {@link Joiner} policies, kept out of the simulation engines.
 *
 * <p>It previously lived as a {@code public static} method on the modern simulator, which meant a
 * class responsible for running a warehouse shift also owned unrelated teaching material. Demos and
 * production paths should not share a class.
 *
 * <p>The two policies shown answer different questions:
 * <ul>
 *   <li>{@code allSuccessfulOrThrow()} — "I need every result." Fails fast and cancels siblings.</li>
 *   <li>{@code anySuccessfulOrThrow()} — "I need one result, whoever is quickest." Cancels the
 *       losers the moment a winner appears.</li>
 * </ul>
 */
public final class JoinerApiDemo {

    private final PrintStream out;

    public JoinerApiDemo(PrintStream out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    public JoinerApiDemo() {
        this(System.out);
    }

    public void run() throws InterruptedException {
        out.println();
        out.println("─".repeat(78));
        out.println("▶  StructuredTaskScope Joiner policies");
        out.println("─".repeat(78));

        allSuccessful();
        firstSuccessful();
    }

    /** Fan-out where every branch is required: {@code join()} hands back the results in fork order. */
    private void allSuccessful() throws InterruptedException {
        List<Callable<String>> routePlanners = List.of(
                planner("route-A", Duration.ofMillis(100)),
                planner("route-B", Duration.ofMillis(150)),
                planner("route-C", Duration.ofMillis(80)));

        try (var scope = StructuredTaskScope.open(Joiner.<String>allSuccessfulOrThrow())) {
            routePlanners.forEach(scope::fork);
            // JDK 26 returns List<String> directly — no .stream().map(Subtask::get) ceremony.
            List<String> routes = scope.join();
            out.printf("   allSuccessfulOrThrow  → %s%n", routes);
        } catch (StructuredTaskScope.FailedException e) {
            out.printf("   allSuccessfulOrThrow  → failed: %s%n", e.getCause());
        }
    }

    /** Race where the first winner is enough; the scope cancels the slower branches automatically. */
    private void firstSuccessful() throws InterruptedException {
        try (var scope = StructuredTaskScope.open(Joiner.<String>anySuccessfulOrThrow())) {
            scope.fork(planner("warehouse-north", Duration.ofMillis(120)));
            scope.fork(planner("warehouse-south", Duration.ofMillis(40)));
            scope.fork(planner("warehouse-east", Duration.ofMillis(200)));

            String winner = scope.join();
            out.printf("   anySuccessfulOrThrow  → %s (slower branches already cancelled)%n", winner);
        } catch (StructuredTaskScope.FailedException e) {
            out.printf("   anySuccessfulOrThrow  → all branches failed: %s%n", e.getCause());
        }
    }

    private static Callable<String> planner(String label, Duration latency) {
        return () -> {
            Thread.sleep(latency);
            return label;
        };
    }
}



