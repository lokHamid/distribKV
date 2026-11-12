package node_runner;

import consensus_core.node.RaftNode;
import kv_store.KVStateMachine;
import kv_store.command.CommandType;
import kv_store.command.KVCommand;
import kv_store.command.KVCommandCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class ClusterSupervisorTest {

    @TempDir
    Path tempDir;

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static boolean await(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(30);
        }
        return condition.getAsBoolean();
    }

    private ClusterSupervisor startCluster(int n, Path root) throws Exception {
        int baseRaft = 20000 + (freePort() % 20000);   // avoid the ephemeral range + port overflow
        int baseTelemetry = baseRaft + 1000;           // disjoint from raft
        int baseAdmin = baseRaft + 2000;               // disjoint from both
        Map<String, Integer> raftPorts = new LinkedHashMap<>();
        Map<String, Integer> telemetryPorts = new LinkedHashMap<>();
        Map<String, Integer> adminPorts = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            raftPorts.put("node-" + (i + 1), baseRaft + i * 10);
            telemetryPorts.put("node-" + (i + 1), baseTelemetry + i * 10);
            adminPorts.put("node-" + (i + 1), baseAdmin + i * 10);
        }
        Map<String, String> addresses = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            addresses.put("node-" + (i + 1), "ws://127.0.0.1:" + raftPorts.get("node-" + (i + 1)));
        }

        ClusterSupervisor supervisor = new ClusterSupervisor();
        for (int i = 0; i < n; i++) {
            String id = "node-" + (i + 1);
            Map<String, String> peers = new LinkedHashMap<>(addresses);
            peers.remove(id);
            supervisor.addNode(new NodeRunnerConfig(id, peers, root.resolve(id),
                    raftPorts.get(id), telemetryPorts.get(id), adminPorts.get(id), 60, 200, 400, 100, 0));
        }
        supervisor.startAll();
        return supervisor;
    }

    private static String awaitLeader(ClusterSupervisor s, long timeoutMs, String excluded)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Optional<String> l = s.currentLeaderId();
            if (l.isPresent() && !l.get().equals(excluded)) {
                return l.get();
            }
            Thread.sleep(30);
        }
        throw new AssertionError("no leader within " + timeoutMs + " ms"
                + (excluded != null ? " (other than " + excluded + ")" : ""));
    }

    private void assertCommit(ClusterSupervisor supervisor) throws Exception {
        byte[] cmd = KVCommandCodec.encode(
                new KVCommand(CommandType.PUT, "k" + System.nanoTime(), "v"));
        String key = KVCommandCodec.decode(cmd).getKey();

        long deadline = System.currentTimeMillis() + 20_000;
        boolean ok = false;
        while (!ok && System.currentTimeMillis() < deadline) {
            Optional<String> id = supervisor.currentLeaderId();
            if (id.isEmpty()) { Thread.sleep(50); continue; }

            RaftNode leader = supervisor.runner(id.get()).getNode();
            long index;
            try {
                index = leader.propose(cmd);
            } catch (Exception e) {
                Thread.sleep(100);
                continue;
            }
            if (index < 1) { Thread.sleep(100); continue; }   // not leader anymore, retry

            ok = await(() -> leader.getCommitIndex() >= index, 3000)
                    && await(() -> ((KVStateMachine) leader.getStateMachine())
                    .getStore().get(key) != null, 3000);
        }
        assertTrue(ok, "write should commit and be applied");
    }

    @Test
    void electsLeaderCommitsAndRecoversFromLeaderKill() throws Exception {
        ClusterSupervisor supervisor = startCluster(3, tempDir.resolve("c1"));
        try {
            String initialLeader = awaitLeader(supervisor, 15_000, null);
            assertCommit(supervisor);

            supervisor.controller().killNode(initialLeader);
            assertFalse(supervisor.runner(initialLeader).isRunning());

            awaitLeader(supervisor, 15_000, initialLeader);
            assertCommit(supervisor);
        } finally {
            supervisor.stopAll();
        }
    }

    @Test
    void restartBringsNodeBackAndRejoins() throws Exception {
        ClusterSupervisor supervisor = startCluster(3, tempDir.resolve("c2"));
        try {
            String leader = awaitLeader(supervisor, 15_000, null);
            supervisor.controller().killNode(leader);
            awaitLeader(supervisor, 15_000, leader);

            supervisor.controller().restartNode(leader);
            assertTrue(await(() -> supervisor.runner(leader).isRunning(), 5000));
            awaitLeader(supervisor, 15_000, null);
        } finally {
            supervisor.stopAll();
        }
    }
}
