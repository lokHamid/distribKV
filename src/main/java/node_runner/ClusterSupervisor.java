// node_runner/ClusterSupervisor.java
package node_runner;

import chaos_harness.ChaosController;
import chaos_harness.ChaosRules;
import chaos_harness.ChaosScenario;
import chaos_harness.ChaosScenarioRunner;
import consensus_core.api.EventType;
import consensus_core.api.RaftEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Dev/deploy mode supervisor that owns a whole cluster. Each node is a
 * {@link NodeRunner}; all of them share a single {@link ChaosRules} so a
 * partition/delay/drop is applied consistently cluster-wide. Exposes a
 * {@link ChaosController} that maps onto the node lifecycles (kill/restart) and
 * the shared fault rules (partition/heal/delay/drop/scenario).
 */
public class ClusterSupervisor {

    private final Map<String, NodeRunner> runners = new LinkedHashMap<>();
    private final List<String> allNodeIds = new ArrayList<>();
    private final ChaosRules rules = new ChaosRules();
    private ChaosAdminServer adminServer;

    public NodeRunner addNode(NodeRunnerConfig config) {
        NodeRunner runner = new NodeRunner(config, rules);
        runners.put(config.getNodeId(), runner);
        allNodeIds.add(config.getNodeId());
        return runner;
    }

    public void startAll() {
        runners.values().forEach(NodeRunner::start);
    }

    public void stopAll() {
        runners.values().forEach(NodeRunner::stop);
        stopAdminServer();
    }

    public Collection<NodeRunner> runners() { return runners.values(); }
    public NodeRunner runner(String nodeId) { return runners.get(nodeId); }
    public ChaosRules rules() { return rules; }
    public int size() { return runners.size(); }

    public Optional<String> currentLeaderId() {
        return runners.values().stream()
                .filter(NodeRunner::isRunning)
                .map(NodeRunner::getNode)
                .filter(n -> n != null && n.isLeader())
                .map(n -> n.getNodeId())
                .findFirst();
    }

    public ChaosController controller() {
        return new ClusterController();
    }

    // ------------------------------------------------------------------ admin/control plane

    /** Builds the cluster description (name, cluster admin port, per-node endpoints). */
    public ClusterDescriptor clusterDescriptor(String name, int adminPort) {
        List<ClusterDescriptor.NodeEndpoint> endpoints = new ArrayList<>();
        for (NodeRunner runner : runners.values()) {
            endpoints.add(new ClusterDescriptor.NodeEndpoint(
                    runner.getNodeId(), runner.getHost(), runner.getRaftPort(),
                    runner.getTelemetryPort(), runner.getAdminPort()));
        }
        return new ClusterDescriptor(name, adminPort, endpoints);
    }

    /** Starts a single cluster-wide admin server on {@code port} (single-JVM mode). */
    public int startAdminServer(int port) {
        stopAdminServer();
        try {
            adminServer = new ChaosAdminServer(port, clusterAdminBackend());
            adminServer.start();
            return adminServer.getPort();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to start cluster admin server", e);
        }
    }

    public int getAdminPort() {
        return adminServer != null ? adminServer.getPort() : -1;
    }

    public void stopAdminServer() {
        if (adminServer != null) {
            adminServer.stop();
            adminServer = null;
        }
    }

    private AdminBackend clusterAdminBackend() {
        return new AdminBackend() {
            @Override
            public List<NodeStatus> statuses() {
                List<NodeStatus> out = new ArrayList<>();
                for (NodeRunner runner : runners.values()) {
                    out.add(NodeStatus.fromNode(runner.getNodeId(), runner.getNode()));
                }
                return out;
            }
            @Override
            public List<NodeMetrics> metrics() {
                List<NodeMetrics> out = new ArrayList<>();
                for (NodeRunner runner : runners.values()) {
                    out.add(new NodeMetrics(runner.getNodeId(), runner.getMetricsCollector().snapshot()));
                }
                return out;
            }
            @Override
            public ClusterDescriptor cluster() {
                return clusterDescriptor("distrokv", getAdminPort());
            }
            @Override
            public ChaosController controller() {
                return new ClusterController();
            }
            @Override
            public ChaosScenario scenarioByName(String name) {
                return ClusterScenarios.byName(name);
            }
        };
    }

    private void publishAcrossCluster(EventType type, Map<String, String> metadata) {
        for (NodeRunner runner : runners.values()) {
            runner.getTelemetryBus().publish(new RaftEvent(type, "supervisor", 0, System.currentTimeMillis(), metadata));
        }
    }

    private class ClusterController implements ChaosController {

        @Override
        public void killNode(String nodeId) {
            NodeRunner runner = runners.get(nodeId);
            if (runner != null) {
                publishAcrossCluster(EventType.CHAOS_NODE_KILLED, Map.of("nodeId", nodeId));
                runner.stop();
            }
        }

        @Override
        public void restartNode(String nodeId) {
            NodeRunner runner = runners.get(nodeId);
            if (runner != null) {
                runner.start();
                publishAcrossCluster(EventType.CHAOS_NODE_RESTARTED, Map.of("nodeId", nodeId));
            }
        }

        @Override
        public void partition(Collection<String> groupA, Collection<String> groupB) {
            Map<String, Integer> grouping = new HashMap<>();
            int next = 0;
            for (String id : allNodeIds) {
                if (groupA.contains(id)) {
                    grouping.put(id, 0);
                } else if (groupB.contains(id)) {
                    grouping.put(id, 1);
                } else {
                    grouping.put(id, ++next);
                }
            }
            setPartition(grouping);
        }

        @Override
        public void heal() {
            rules.heal();
            publishAcrossCluster(EventType.CHAOS_HEALED, Map.of());
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
            publishAcrossCluster(EventType.CHAOS_PARTITIONED, Map.of("groups", grouping.toString()));
        }

        @Override
        public void runScenario(ChaosScenario scenario) {
            ChaosScenarioRunner.run(this, scenario);
        }
    }
}
