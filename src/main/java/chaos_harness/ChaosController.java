// chaos_harness/ChaosController.java
package chaos_harness;

import java.util.Collection;
import java.util.Map;

/**
 * The administrative (operator-facing) control surface for the cluster
 * (SRS §5.3). Distinct from the client KV API so there's no risk of accidental
 * chaos injection from application code (NFR-SEC-2).
 */
public interface ChaosController {

    /** FR-CHAOS-1: forcibly stop the given node process (graceful process/kernel stop). */
    void killNode(String nodeId);

    /** FR-CHAOS-2: restart a killed node so it rejoins and catches up. */
    void restartNode(String nodeId);

    /** FR-CHAOS-3: isolate two groups from each other. */
    void partition(Collection<String> groupA, Collection<String> groupB);

    /** FR-CHAOS-4: restore full connectivity. */
    void heal();

    /** FR-CHAOS-5 (S): inject a one-way delay between two nodes. */
    void delayLink(String nodeA, String nodeB, long millis);

    /** FR-CHAOS-5 (S): inject a one-way packet drop probability between two nodes. */
    void dropRate(String nodeA, String nodeB, double probability);

    /** Set an arbitrary partition (nodeId -> group) for more than two groups. */
    void setPartition(Map<String, Integer> grouping);

    /** FR-CHAOS-6 (S): execute a scripted scenario. */
    void runScenario(ChaosScenario scenario);
}
