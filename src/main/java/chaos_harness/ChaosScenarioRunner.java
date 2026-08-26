// chaos_harness/ChaosScenarioRunner.java
package chaos_harness;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Executes a {@link ChaosScenario}: schedules each step on a background
 * scheduler and applies it to a {@link ChaosController} when its offset elapses
 * (FR-CHAOS-6).
 */
public final class ChaosScenarioRunner {

    private ChaosScenarioRunner() {
    }

    public static List<ScheduledFuture<?>> run(ChaosController controller, ChaosScenario scenario) {
        return run(controller, scenario, Executors.newSingleThreadScheduledExecutor());
    }

    public static List<ScheduledFuture<?>> run(ChaosController controller, ChaosScenario scenario,
                                               ScheduledExecutorService scheduler) {
        List<ScheduledFuture<?>> futures = new ArrayList<>();
        for (ChaosScenario.Step step : scenario.getSteps()) {
            futures.add(scheduler.schedule(() -> apply(controller, step), step.offsetMillis(), TimeUnit.MILLISECONDS));
        }
        return futures;
    }

    @SuppressWarnings("unchecked")
    private static void apply(ChaosController controller, ChaosScenario.Step step) {
        switch (step.action()) {
            case KILL -> controller.killNode((String) step.args().get("nodeId"));
            case RESTART -> controller.restartNode((String) step.args().get("nodeId"));
            case PARTITION -> controller.partition(
                    (List<String>) step.args().get("groupA"), (List<String>) step.args().get("groupB"));
            case HEAL -> controller.heal();
            case DELAY_LINK -> controller.delayLink(
                    (String) step.args().get("nodeA"), (String) step.args().get("nodeB"),
                    ((Number) step.args().get("millis")).longValue());
            case DROP_RATE -> controller.dropRate(
                    (String) step.args().get("nodeA"), (String) step.args().get("nodeB"),
                    ((Number) step.args().get("probability")).doubleValue());
            case SET_PARTITION -> controller.setPartition((Map<String, Integer>) step.args().get("grouping"));
            default -> {
            }
        }
    }
}
