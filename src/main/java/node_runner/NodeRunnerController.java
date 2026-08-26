// node_runner/NodeRunnerController.java
package node_runner;

import chaos_harness.ChaosController;
import chaos_harness.ChaosRules;
import chaos_harness.ChaosScenario;
import chaos_harness.ChaosScenarioRunner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ChaosController} scoped to a single {@link NodeRunner}. Used in
 * separate-JVM mode: partition/heal/delay/drop mutate this process's
 * {@link ChaosRules} (an orchestrator sends the same command to every node to
 * keep them consistent), and kill/restart only affect the node this JVM hosts.
 */
public final class NodeRunnerController implements ChaosController {

    private final NodeRunner runner;
    private final ChaosRules rules;

    public NodeRunnerController(NodeRunner runner, ChaosRules rules) {
        this.runner = runner;
        this.rules = rules;
    }

    @Override
    public void killNode(String nodeId) {
        if (nodeId.equals(runner.getNodeId())) {
            runner.stop();
        }
    }

    @Override
    public void restartNode(String nodeId) {
        if (nodeId.equals(runner.getNodeId())) {
            runner.start();
        }
    }

    @Override
    public void partition(Collection<String> groupA, Collection<String> groupB) {
        Set<String> members = new HashSet<>(runner.getPeerIds());
        members.add(runner.getNodeId());
        int next = 2;
        Map<String, Integer> grouping = new HashMap<>();
        for (String id : members) {
            if (groupA.contains(id)) {
                grouping.put(id, 0);
            } else if (groupB.contains(id)) {
                grouping.put(id, 1);
            } else {
                grouping.put(id, next++);
            }
        }
        setPartition(grouping);
    }

    @Override
    public void heal() {
        rules.heal();
    }

    @Override
    public void delayLink(String nodeA, String nodeB, long millis) {
        rules.delayLink(nodeA, nodeB, millis);
    }

    @Override
    public void dropRate(String nodeA, String nodeB, double probability) {
        rules.dropRate(nodeA, nodeB, probability);
    }

    @Override
    public void setPartition(Map<String, Integer> grouping) {
        rules.setPartition(grouping);
    }

    @Override
    public void runScenario(ChaosScenario scenario) {
        ChaosScenarioRunner.run(this, scenario);
    }
}
