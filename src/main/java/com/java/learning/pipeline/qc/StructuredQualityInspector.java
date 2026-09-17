package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;

import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;

/**
 * Fan-out via a nested {@link StructuredTaskScope}: both checks run on virtual threads owned by the
 * calling picker.
 *
 * <p>The contrast with {@link ExecutorQualityInspector} is the whole point of this project:
 *
 * <ul>
 *   <li><strong>No pool to size.</strong> Virtual threads are created per check and discarded. There
 *       is no thread-starvation deadlock to reason about, because nesting scopes never competes for
 *       a fixed thread budget.</li>
 *   <li><strong>No resources to close.</strong> {@code try-with-resources} on the scope guarantees
 *       both checks have terminated before {@code inspect} returns — the scope cannot leak a thread.</li>
 *   <li><strong>Automatic sibling cancellation.</strong> {@code allSuccessfulOrThrow()} cancels the
 *       remaining check the instant one fails, instead of waiting out a doomed inspection.</li>
 *   <li><strong>Observability.</strong> The named scope shows the picker/check parent-child tree in
 *       {@code jcmd <pid> Thread.dump_to_file -format=json}.</li>
 * </ul>
 */
public final class StructuredQualityInspector implements QualityInspector {

    private static final int WEIGHT_CHECK = 0;
    private static final int LABEL_SCAN = 1;

    @Override
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

    @Override
    public String toString() {
        return "structured fan-out (nested scope, virtual threads)";
    }
}

