// node_runner/ClusterScenarios.java
package node_runner;

import chaos_harness.ChaosScenario;

import java.util.List;
import java.util.Map;

/**
 * The built-in scripted chaos scenarios, shared by the {@link ClusterSupervisor}
 * admin endpoint and per-node admin servers. Both assume the default 3-node
 * cluster ({@code node-1}/{@code node-2}/{@code node-3}); offsets are ms from
 * the start of the scenario.
 */
public final class ClusterScenarios {

    private ClusterScenarios() {
    }

    /** Minority-majority split, then heal. */
    public static ChaosScenario splitBrain() {
        return ChaosScenario.builder("split-brain")
                .partitionAt(1000, List.of("node-1"), List.of("node-2", "node-3"))
                .healAt(5000)
                .build();
    }

    /** Kill the leader, restart it, isolate it once more, then heal. */
    public static ChaosScenario leaderChurn() {
        return ChaosScenario.builder("leader-churn")
                .killAt(1000, "node-1")
                .restartAt(5000, "node-1")
                .setPartitionAt(8000, Map.of("node-1", 0, "node-2", 1, "node-3", 1))
                .healAt(12000)
                .build();
    }

    public static ChaosScenario byName(String name) {
        return switch (name.toLowerCase()) {
            case "split-brain" -> splitBrain();
            case "leader-churn" -> leaderChurn();
            case "full" -> ChaosScenario.builder("full")
                    .killAt(1000, "node-1")
                    .restartAt(4000, "node-1")
                    .partitionAt(7000, List.of("node-1"), List.of("node-2", "node-3"))
                    .delayAt(9000, "node-2", "node-3", 250)
                    .dropAt(9500, "node-3", "node-2", 0.5)
                    .healAt(12000)
                    .build();
            default -> throw new IllegalArgumentException("unknown scenario: " + name);
        };
    }

    public static java.util.List<String> names() {
        return java.util.List.of("split-brain", "leader-churn", "full");
    }
}
