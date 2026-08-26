// node_runner/NodeRunner.java
package node_runner;

import chaos_harness.ChaosController;
import chaos_harness.ChaosRules;
import chaos_harness.ChaosTransportClient;
import consensus_core.node.RaftConfig;
import consensus_core.node.RaftNode;
import consensus_core.log.RaftLog;
import consensus_core.state.PersistentState;
import consensus_core.state.VolatileState;
import consensus_core.transport.RaftRpcServer;
import consensus_core.transport.WebSocketTransportClient;
import kv_store.KVStateMachine;
import kv_store.KVStore;
import telemetry.TelemetryBus;
import telemetry.metrics.MetricsCollector;
import telemetry.server.TelemetryWebSocketServer;

import java.util.Set;

/**
 * Boots and supervises one logical node as a self-contained unit: the Raft
 * engine, the KV state machine, the telemetry bus/metrics server, and its
 * chaos-filtered outbound transport. It can be stopped (simulating a killed
 * process) and restarted (reloading persisted state) without losing its
 * configuration — this is what a {@link ClusterSupervisor} and a single-process
 * deployment both use.
 */
public class NodeRunner {

    private final NodeRunnerConfig config;
    private final ChaosRules rules;

    // telemetry state survives node restart so metric history is retained
    private final TelemetryBus telemetryBus = new TelemetryBus();
    private final MetricsCollector metricsCollector = new MetricsCollector();

    private RaftNode node;
    private RaftRpcServer rpcServer;
    private TelemetryWebSocketServer telemetryServer;
    private ChaosTransportClient chaosTransport;
    private PersistentState persistentState;
    private RaftLog raftLog;
    private KVStateMachine stateMachine;
    private ChaosAdminServer adminServer;

    public NodeRunner(NodeRunnerConfig config, ChaosRules rules) {
        this.config = config;
        this.rules = rules;
        telemetryBus.subscribe(metricsCollector);
    }

    /** Constructs all components and starts the node, its RPC server and its telemetry server. */
    public synchronized void start() {
        if (node != null) {
            return;
        }
        RaftConfig raftConfig = new RaftConfig(
                config.getNodeId(),
                config.getPeerAddresses().keySet(),
                config.getDataDir(),
                config.getHeartbeatIntervalMs(),
                config.getMinElectionTimeoutMs(),
                config.getMaxElectionTimeoutMs(),
                config.getMaxEntriesPerAppend(),
                config.getSnapshotThreshold());

        persistentState = new PersistentState(config.getDataDir().resolve("persistent.bin"));
        raftLog = new RaftLog(config.getDataDir().resolve("raft.log"));
        stateMachine = new KVStateMachine(new KVStore());
        chaosTransport = new ChaosTransportClient(
                config.getNodeId(),
                new WebSocketTransportClient(config.getPeerAddresses()),
                rules,
                telemetryBus);
        node = new RaftNode(raftConfig, persistentState, new VolatileState(), raftLog,
                stateMachine, chaosTransport, telemetryBus);
        rpcServer = new RaftRpcServer(node, config.getRaftPort());
        telemetryServer = new TelemetryWebSocketServer(telemetryBus, config.getTelemetryPort());

        rpcServer.start();
        telemetryServer.start();
        node.start();
    }

    /** Stops the node and its servers, simulating a killed process. Idempotent. */
    public synchronized void stop() {
        if (node == null) {
            return;
        }
        node.stop();
        try {
            rpcServer.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            telemetryServer.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        chaosTransport.close();
        persistentState.close();
        raftLog.close();

        rpcServer = null;
        telemetryServer = null;
        chaosTransport = null;
        persistentState = null;
        raftLog = null;
        stateMachine = null;
        node = null;
    }

    public synchronized boolean isRunning() {
        return node != null;
    }

    public String getNodeId() { return config.getNodeId(); }
    public String getHost() { return config.getHost(); }
    public int getAdminPort() { return config.getAdminPort(); }
    public ChaosRules getRules() { return rules; }
    public synchronized RaftNode getNode() { return node; }
    public TelemetryBus getTelemetryBus() { return telemetryBus; }
    public MetricsCollector getMetricsCollector() { return metricsCollector; }
    public int getRaftPort() { return config.getRaftPort(); }
    public int getTelemetryPort() { return config.getTelemetryPort(); }

    /** Peer ids in the cluster (excludes this node). */
    public Set<String> getPeerIds() { return config.getPeerAddresses().keySet(); }

    /** Per-node admin server (used in separate-JVM mode). Returns the bound port. */
    public synchronized int startNodeAdmin() {
        if (adminServer != null) {
            return adminServer.getPort();
        }
        try {
            adminServer = new ChaosAdminServer(config.getAdminPort(), nodeAdminBackend());
            adminServer.start();
            return adminServer.getPort();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to start per-node admin server", e);
        }
    }

    public synchronized void stopNodeAdmin() {
        if (adminServer != null) {
            adminServer.stop();
            adminServer = null;
        }
    }

    private AdminBackend nodeAdminBackend() {
        return new AdminBackend() {
            @Override
            public java.util.List<NodeStatus> statuses() {
                return java.util.List.of(NodeStatus.fromNode(NodeRunner.this.getNodeId(), NodeRunner.this.getNode()));
            }
            @Override
            public java.util.List<NodeMetrics> metrics() {
                return java.util.List.of(new NodeMetrics(NodeRunner.this.getNodeId(), metricsCollector.snapshot()));
            }
            @Override
            public ClusterDescriptor cluster() {
                return new ClusterDescriptor("node-" + NodeRunner.this.getNodeId(), java.util.List.of(
                        new ClusterDescriptor.NodeEndpoint(getNodeId(), getHost(), getRaftPort(), getTelemetryPort(), getAdminPort())));
            }
            @Override
            public ChaosController controller() {
                return new NodeRunnerController(NodeRunner.this, rules);
            }
            @Override
            public chaos_harness.ChaosScenario scenarioByName(String name) {
                return ClusterScenarios.byName(name);
            }
        };
    }
}
