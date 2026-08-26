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

    private void assertCommit(ClusterSupervisor supervisor) throws Exception {
        String leaderId = supervisor.currentLeaderId().orElseThrow();
        RaftNode leader = supervisor.runner(leaderId).getNode();
        byte[] cmd = KVCommandCodec.encode(new KVCommand(CommandType.PUT, "k" + System.nanoTime(), "v"));
        long index = leader.propose(cmd);
        assertTrue(index >= 1);
        assertTrue(await(() -> leader.getCommitIndex() >= index, 5000), "write should commit");
        assertTrue(await(() -> ((KVStateMachine) leader.getStateMachine()).getStore().get(
                KVCommandCodec.decode(cmd).getKey()) != null, 5000), "state machine should apply");
    }

    @Test
    void electsLeaderCommitsAndRecoversFromLeaderKill() throws Exception {
        ClusterSupervisor supervisor = startCluster(3, tempDir.resolve("c1"));
        try {
            assertTrue(await(() -> supervisor.currentLeaderId().isPresent(), 6000), "leader should emerge");
            String initialLeader = supervisor.currentLeaderId().orElseThrow();
            assertEquals(initialLeader, supervisor.currentLeaderId().orElse(null));

            assertCommit(supervisor);

            supervisor.controller().killNode(initialLeader);
            assertFalse(supervisor.runner(initialLeader).isRunning());

            assertTrue(await(() -> {
                Optional<String> l = supervisor.currentLeaderId();
                return l.isPresent() && !l.get().equals(initialLeader);
            }, 6000), "a new leader should be elected after the leader is killed");

            assertCommit(supervisor);
        } finally {
            supervisor.stopAll();
        }
    }

    @Test
    void restartBringsNodeBackAndRejoins() throws Exception {
        ClusterSupervisor supervisor = startCluster(3, tempDir.resolve("c2"));
        try {
            assertTrue(await(() -> supervisor.currentLeaderId().isPresent(), 6000), "leader should emerge");
            String leader = supervisor.currentLeaderId().orElseThrow();
            supervisor.controller().killNode(leader);
            assertTrue(await(() -> supervisor.currentLeaderId().filter(id -> !id.equals(leader)).isPresent(), 6000));

            supervisor.controller().restartNode(leader);
            assertTrue(await(() -> supervisor.runner(leader).isRunning(), 3000));
            assertTrue(await(() -> supervisor.currentLeaderId().isPresent(), 6000));
        } finally {
            supervisor.stopAll();
        }
    }
}
