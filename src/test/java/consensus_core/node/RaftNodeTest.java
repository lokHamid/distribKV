package consensus_core.node;

import consensus_core.api.TelemetryPublisher;
import consensus_core.log.LogEntry;
import consensus_core.log.RaftLog;
import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import consensus_core.state.PersistentState;
import consensus_core.state.VolatileState;
import consensus_core.transport.TransportClient;
import kv_store.KVStateMachine;
import kv_store.KVStore;
import kv_store.command.CommandType;
import kv_store.command.KVCommand;
import kv_store.command.KVCommandCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {

    @TempDir
    Path tempDir;

    private static final class NoopTransport implements TransportClient {
        @Override
        public CompletableFuture<RequestVoteReply> sendRequestVote(String target, RequestVoteArgs a) {
            return CompletableFuture.completedFuture(new RequestVoteReply(a.getTerm(), false));
        }
        @Override
        public CompletableFuture<AppendEntriesReply> sendAppendEntries(String target, AppendEntriesArgs a) {
            return CompletableFuture.completedFuture(new AppendEntriesReply(a.getTerm(), false, -1));
        }
        @Override
        public CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String target, InstallSnapshotArgs a) {
            return CompletableFuture.completedFuture(new InstallSnapshotReply(a.getTerm(), false));
        }
    }

    private RaftNode newNode(RaftLog log) {
        RaftConfig config = new RaftConfig("node-1", Collections.emptySet(), tempDir,
                30, 100, 200, 100, 0);
        return new RaftNode(config, new PersistentState(tempDir.resolve("state.bin")),
                new VolatileState(), log, new KVStateMachine(new KVStore()), new NoopTransport(), null);
    }

    private RaftNode newNode() {
        return newNode(new RaftLog(tempDir.resolve("raft.log")));
    }

    @Test
    void singleNodeBecomesLeaderAndCommits() throws Exception {
        RaftNode node = newNode();
        node.start();
        try {
            long deadline = System.currentTimeMillis() + 3000;
            while (!node.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(NodeRole.LEADER, node.getRole());
            assertEquals("node-1", node.getLeaderId());
            assertEquals(1, node.getCurrentTerm());

            byte[] command = KVCommandCodec.encode(new KVCommand(CommandType.PUT, "hello", "world"));
            long index = node.propose(command);
            assertTrue(index >= 1);

            while (node.getCommitIndex() < index && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(node.getCommitIndex() >= index);
            assertEquals("world", ((KVStateMachine) node.getStateMachine()).getStore().get("hello"));
        } finally {
            node.stop();
        }
    }

    @Test
    void rejectOlderTermVote() throws Exception {
        RaftNode node = newNode();
        node.start();
        try {
            // advance the node to term 1 by granting one vote, then a term-0 request must fail
            assertTrue(node.handleRequestVote(new RequestVoteArgs(1, "c1", 0, 0)).isVoteGranted());
            RequestVoteReply reply = node.handleRequestVote(new RequestVoteArgs(0, "c2", 0, 0));
            assertFalse(reply.isVoteGranted());
            assertTrue(reply.getTerm() >= 1);
        } finally {
            node.stop();
        }
    }

    @Test
    void grantVoteForCandidateWithUpToDateLog() {
        RaftNode voter = newNode();
        voter.start();
        try {
            RequestVoteReply reply = voter.handleRequestVote(new RequestVoteArgs(1, "candidate", 0, 0));
            assertTrue(reply.isVoteGranted());
            assertEquals("candidate", voter.getVotedFor());
        } finally {
            voter.stop();
        }
    }

    @Test
    void denySecondVoteInSameTerm() {
        RaftNode voter = newNode();
        voter.start();
        try {
            assertTrue(voter.handleRequestVote(new RequestVoteArgs(1, "c1", 0, 0)).isVoteGranted());
            assertFalse(voter.handleRequestVote(new RequestVoteArgs(1, "c2", 0, 0)).isVoteGranted());
        } finally {
            voter.stop();
        }
    }

    @Test
    void denyCandidateWithStaleLog() {
        // voter has a longer/same-term log, so a shorter candidate is denied
        RaftLog voterLog = new RaftLog(tempDir.resolve("stale.log"));
        for (int i = 1; i <= 5; i++) {
            voterLog.append(new LogEntry(i, 5, ("v" + i).getBytes()));
        }
        RaftNode voter = newNode(voterLog);
        voter.start();
        try {
            RequestVoteReply reply = voter.handleRequestVote(new RequestVoteArgs(6, "candidate", 3, 5));
            assertFalse(reply.isVoteGranted());
            assertEquals(6, voter.getCurrentTerm());
        } finally {
            voter.stop();
        }
    }
}
