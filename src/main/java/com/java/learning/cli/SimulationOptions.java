package com.java.learning.cli;

import com.java.learning.config.WarehouseConfig;

import java.time.Duration;
import java.util.Objects;

/**
 * Fully-resolved options for a run: which engines, tuned with what parameters.
 *
 * @param mode     engine selection
 * @param config   shift parameters, defaults overlaid with any CLI overrides
 * @param showDemo whether to append the structured-concurrency API demo
 */
public record SimulationOptions(SimulationMode mode, WarehouseConfig config, boolean showDemo) {

    public SimulationOptions {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(config, "config");
    }

    /**
     * Parses {@code [mode] [--option=value]...}.
     *
     * <p>Overrides exist so the two engines can be pushed to interesting load levels without a
     * rebuild — raise {@code --producers}/{@code --consumers} into the thousands and the difference
     * between platform and virtual threads stops being academic.
     *
     * @throws IllegalArgumentException with a user-facing message if anything is malformed
     */
    public static SimulationOptions parse(String[] args) {
        Objects.requireNonNull(args, "args");

        SimulationMode mode = SimulationMode.ALL;
        WarehouseConfig config = WarehouseConfig.defaults();
        boolean showDemo = true;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i].trim();
            if (arg.isEmpty()) {
                continue;
            }

            if (!arg.startsWith("--")) {
                if (i != 0) {
                    throw new IllegalArgumentException(
                            "Mode must be the first argument, but found '" + arg + "' at position " + i);
                }
                mode = SimulationMode.parse(arg);
                continue;
            }

            if (arg.equals("--no-demo")) {
                showDemo = false;
                continue;
            }

            int separator = arg.indexOf('=');
            if (separator < 0) {
                throw new IllegalArgumentException("Option '" + arg + "' requires a value, e.g. " + arg + "=10");
            }
            String key = arg.substring(2, separator);
            String value = arg.substring(separator + 1);

            config = switch (key) {
                case "producers" -> config.withProducers(positiveInt(key, value));
                case "consumers" -> config.withConsumers(positiveInt(key, value));
                case "buffer" -> config.withBufferCapacity(positiveInt(key, value));
                case "orders" -> config.withOrderQuota(positiveInt(key, value));
                case "intake-delay-ms" -> config.withIntakeDelay(Duration.ofMillis(nonNegativeInt(key, value)));
                case "timeout-seconds" -> config.withShiftTimeout(Duration.ofSeconds(positiveInt(key, value)));
                default -> throw new IllegalArgumentException("Unknown option '--" + key + "'");
            };
        }

        return new SimulationOptions(mode, config, showDemo);
    }

    public static String usage() {
        return """
               Usage: java --enable-preview -jar java-learning.jar [mode] [options]

                 mode                    %s   (default: all)

               Options:
                 --producers=N           intake workers            (default: 5)
                 --consumers=N           picker workers            (default: 8)
                 --buffer=N              conveyor capacity         (default: 20)
                 --orders=N              orders to produce         (default: 100)
                 --intake-delay-ms=N     pause between orders      (default: 5)
                 --timeout-seconds=N     hard shift deadline       (default: 30)
                 --no-demo               skip the Joiner API demo

               Examples:
                 modern --orders=5000 --producers=200 --consumers=500
                 all --orders=200 --no-demo
               """.formatted(SimulationMode.supported());
    }

    private static int positiveInt(String key, String value) {
        int parsed = parseInt(key, value);
        if (parsed <= 0) {
            throw new IllegalArgumentException("--" + key + " must be positive but was " + parsed);
        }
        return parsed;
    }

    private static int nonNegativeInt(String key, String value) {
        int parsed = parseInt(key, value);
        if (parsed < 0) {
            throw new IllegalArgumentException("--" + key + " must not be negative but was " + parsed);
        }
        return parsed;
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " expects an integer but got '" + value + "'");
        }
    }
}

