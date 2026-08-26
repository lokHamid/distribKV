// node_runner/AdminBackend.java
package node_runner;

import chaos_harness.ChaosController;
import chaos_harness.ChaosScenario;

import java.util.List;

/**
 * The data the {@link ChaosAdminServer} serves. A single-JVM cluster provides
 * one backend spanning all nodes; a per-node process provides one for itself.
 */
public interface AdminBackend {

    /** Current status of every node it manages (possibly a single node). */
    List<NodeStatus> statuses();

    /** Current metrics snapshot of every node it manages. */
    List<NodeMetrics> metrics();

    /** Cluster descriptor, or {@code null} if not applicable. */
    ClusterDescriptor cluster();

    /** The chaos controller commands are applied to. */
    ChaosController controller();

    /** Resolves a built-in scenario by name (throws {@link IllegalArgumentException} if unknown). */
    ChaosScenario scenarioByName(String name);
}
