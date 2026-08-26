// node_runner/ClusterSupervisorMain.java
package node_runner;

import consensus_core.node.RaftNode;
import kv_store.KVStateMachine;
import kv_store.command.CommandType;
import kv_store.command.KVCommand;
import kv_store.command.KVCommandCodec;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runnable demo: boots a whole cluster of {@link NodeRunner}s in one JVM (each
 * node is a logical process with its own ports/data dir), waits for an election,
 * and exposes an interactive console for status, writes and chaos commands.
 *
 * <p>Connect a dashboard to any node's telemetry port to observe the cluster in
 * real time. Example commands: {@code status}, {@code put a 1}, {@code get a},
 * {@code partition node-1 | node-2,node-3}, {@code heal}, {@code kill node-1},
 * {@code restart node-1}, {@code scenario leader-churn}, {@code quit}.
 */
public final class ClusterSupervisorMain {

    private static final int BASE_RAFT_PORT = 7200;
    private static final int BASE_TELEMETRY_PORT = 8200;
    private static final int BASE_ADMIN_PORT = 9200;
    private static final int CLUSTER_ADMIN_PORT = 9000;
    private static final int PORT_STEP = 20;

    public static void main(String[] args) throws Exception {
        int nodeCount = args.length > 0 ? Integer.parseInt(args[0]) : 3;
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count must be >= 1");
        }

        Path root = Files.createTempDirectory("distrokv-cluster");
        ClusterSupervisor supervisor = new ClusterSupervisor();

        Map<String, Integer> raftPort = new LinkedHashMap<>();
        Map<String, Integer> telemetryPort = new LinkedHashMap<>();
        Map<String, Integer> adminPort = new LinkedHashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            String id = "node-" + (i + 1);
            raftPort.put(id, BASE_RAFT_PORT + i * PORT_STEP);
            telemetryPort.put(id, BASE_TELEMETRY_PORT + i * PORT_STEP);
            adminPort.put(id, BASE_ADMIN_PORT + i * PORT_STEP);
        }

        Map<String, String> allAddresses = new LinkedHashMap<>();
        for (int i = 0; i < nodeCount; i++) {
            String id = "node-" + (i + 1);
            allAddresses.put(id, "ws://127.0.0.1:" + raftPort.get(id));
        }

        for (int i = 0; i < nodeCount; i++) {
            String id = "node-" + (i + 1);
            Map<String, String> peers = new LinkedHashMap<>(allAddresses);
            peers.remove(id);
            supervisor.addNode(new NodeRunnerConfig(
                    id, peers, root.resolve(id),
                    raftPort.get(id), telemetryPort.get(id), adminPort.get(id),
                    60, 200, 400, 100, 0));
        }

        // expose a cluster-wide control plane for the dashboard
        supervisor.startAdminServer(CLUSTER_ADMIN_PORT);

        // write a machine-readable descriptor so a dashboard can discover endpoints
        ClusterDescriptor descriptor = supervisor.clusterDescriptor("distrokv", supervisor.getAdminPort());
        descriptor.writeTo(root.resolve("cluster.json"));
        Path clusterJson = root.resolve("cluster.json");

        startupBanner(supervisor, telemetryPort, adminPort, supervisor.getAdminPort(), clusterJson);
        supervisor.startAll();

        runConsole(supervisor);
        supervisor.stopAll();
    }

    private static void runConsole(ClusterSupervisor supervisor) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.print("distrokv> ");
            System.out.flush();
            String line = in.readLine();
            if (line == null) {
                break;
            }
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] parts = line.split("\\s+");
            try {
                switch (parts[0].toLowerCase()) {
                    case "quit", "exit" -> {
                        return;
                    }
                    case "status" -> status(supervisor);
                    case "put" -> put(supervisor, parts[1], parts[2]);
                    case "get" -> get(supervisor, parts[1]);
                    case "partition" -> partition(supervisor, line);
                    case "heal" -> supervisor.controller().heal();
                    case "kill" -> supervisor.controller().killNode(parts[1]);
                    case "restart" -> supervisor.controller().restartNode(parts[1]);
                    case "delay" -> supervisor.controller().delayLink(parts[1], parts[2], Long.parseLong(parts[3]));
                    case "drop" -> supervisor.controller().dropRate(parts[1], parts[2], Double.parseDouble(parts[3]));
                    case "scenario" -> supervisor.controller().runScenario(builtInScenario(parts[1]));
                    case "scenarios" -> System.out.println("available: split-brain, leader-churn");
                    case "help" -> System.out.println(
                            "status | put <k> <v> | get <k> | partition <A...> | <B...> | heal | "
                                    + "kill <id> | restart <id> | delay <a> <b> <ms> | drop <a> <b> <p> | "
                                    + "scenario <name> | scenarios | quit");
                    default -> System.out.println("unknown command: " + parts[0]);
                }
            } catch (Exception e) {
                System.out.println("error: " + e.getMessage());
            }
        }
    }

    private static void status(ClusterSupervisor supervisor) {
        System.out.println("Leader: " + supervisor.currentLeaderId().orElse("(none)"));
        System.out.printf("%-10s %-10s %-6s %-8s %-8s %-8s%n", "NODE", "ROLE", "TERM", "LEADER", "COMMIT", "APPLIED");
        for (NodeRunner runner : supervisor.runners()) {
            RaftNode node = runner.getNode();
            if (node == null) {
                System.out.printf("%-10s %-10s (DOWN)%n", runner.getNodeId(), "-");
                continue;
            }
            System.out.printf("%-10s %-10s %-6d %-8s %-8d %-8d%n",
                    runner.getNodeId(), node.getRole(), node.getCurrentTerm(),
                    node.getLeaderId() == null ? "-" : node.getLeaderId(),
                    node.getCommitIndex(), node.getLastApplied());
        }
    }

    private static void put(ClusterSupervisor supervisor, String key, String value) throws InterruptedException {
        RaftNode leader = leaderOrThrow(supervisor);
        byte[] cmd = KVCommandCodec.encode(new KVCommand(CommandType.PUT, key, value));
        long index = leader.propose(cmd);
        if (index < 0) {
            System.out.println("not leader; try again");
            return;
        }
        long deadline = System.currentTimeMillis() + 3000;
        while (leader.getCommitIndex() < index && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        System.out.println("committed " + key + "=" + value + " at index " + index
                + (leader.getCommitIndex() >= index ? "" : " (NOT YET COMMITTED)"));
    }

    private static void get(ClusterSupervisor supervisor, String key) {
        RaftNode leader = leaderOrThrow(supervisor);
        KVStateMachine sm = (KVStateMachine) leader.getStateMachine();
        String value = sm.getStore().get(key);
        System.out.println(key + " = " + value);
    }

    private static RaftNode leaderOrThrow(ClusterSupervisor supervisor) {
        return supervisor.currentLeaderId()
                .map(supervisor::runner)
                .map(NodeRunner::getNode)
                .orElseThrow(() -> new IllegalStateException("no leader yet"));
    }

    private static void partition(ClusterSupervisor supervisor, String line) {
        String[] sides = line.substring("partition".length()).trim().split("\\|");
        if (sides.length != 2) {
            System.out.println("usage: partition <groupA nodes> | <groupB nodes>");
            return;
        }
        supervisor.controller().partition(parseIds(sides[0]), parseIds(sides[1]));
        System.out.println("partition applied");
    }

    private static List<String> parseIds(String csv) {
        List<String> ids = new ArrayList<>();
        for (String s : csv.trim().split(",")) {
            if (!s.isBlank()) {
                ids.add(s.trim());
            }
        }
        return ids;
    }

    private static chaos_harness.ChaosScenario builtInScenario(String name) {
        return ClusterScenarios.byName(name);
    }

    private static void startupBanner(ClusterSupervisor supervisor, Map<String, Integer> telemetryPort,
                                      Map<String, Integer> adminPort, int clusterAdminPort, Path clusterJson) {
        System.out.println("DistribKV — " + supervisor.size() + "-node cluster (single JVM)");
        System.out.println("  cluster admin (HTTP) = " + clusterAdminPort + "   cluster.json = " + clusterJson.toAbsolutePath());
        for (NodeRunner runner : supervisor.runners()) {
            System.out.printf("  %-10s raft=%-5d telemetry=%-5d admin=%-5d data=%s%n",
                    runner.getNodeId(), runner.getRaftPort(), runner.getTelemetryPort(),
                    runner.getAdminPort(), "temp-dir/" + runner.getNodeId());
        }
        System.out.println("Type 'help' for commands, 'quit' to exit.");
    }
}
