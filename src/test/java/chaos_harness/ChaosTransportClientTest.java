package chaos_harness;

import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import consensus_core.transport.TransportClient;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ChaosTransportClientTest {

    private static final String NODE = "node-1";
    private static final String PEER = "node-2";

    private static final class RecordingTransport implements TransportClient {
        final AtomicInteger requestVotes = new AtomicInteger();
        final AtomicInteger appendEntries = new AtomicInteger();

        @Override
        public CompletableFuture<RequestVoteReply> sendRequestVote(String target, RequestVoteArgs a) {
            requestVotes.incrementAndGet();
            return CompletableFuture.completedFuture(new RequestVoteReply(a.getTerm(), true));
        }

        @Override
        public CompletableFuture<AppendEntriesReply> sendAppendEntries(String target, AppendEntriesArgs a) {
            appendEntries.incrementAndGet();
            return CompletableFuture.completedFuture(new AppendEntriesReply(a.getTerm(), true, 0));
        }

        @Override
        public CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String target, InstallSnapshotArgs a) {
            return CompletableFuture.completedFuture(new InstallSnapshotReply(a.getTerm(), true));
        }
    }

    private ChaosTransportClient harness(RecordingTransport delegate, ChaosRules rules) {
        return new ChaosTransportClient(NODE, delegate, rules, null);
    }

    @Test
    void passesThroughWithoutFaults() {
        RecordingTransport delegate = new RecordingTransport();
        ChaosTransportClient harness = harness(delegate, new ChaosRules());
        RequestVoteReply reply = harness.sendRequestVote(PEER, new RequestVoteArgs(1, NODE, 0, 0)).join();
        assertTrue(reply.isVoteGranted());
        assertEquals(1, delegate.requestVotes.get());
        harness.close();
    }

    @Test
    void partitionDropsMessageWithoutCallingDelegate() throws Exception {
        RecordingTransport delegate = new RecordingTransport();
        ChaosRules rules = new ChaosRules();
        rules.setPartition(Map.of(NODE, 0, PEER, 1));
        ChaosTransportClient harness = harness(delegate, rules);

        CompletableFuture<RequestVoteReply> future = harness.sendRequestVote(PEER, new RequestVoteArgs(1, NODE, 0, 0));
        Thread.sleep(100);
        assertFalse(future.isDone(), "dropped message must never complete");
        assertEquals(0, delegate.requestVotes.get());
        harness.close();
    }

    @Test
    void deterministicDropRateDropsEveryMessage() throws Exception {
        RecordingTransport delegate = new RecordingTransport();
        ChaosRules rules = new ChaosRules();
        rules.dropRate(NODE, PEER, 1.0);
        ChaosTransportClient harness = harness(delegate, rules);

        CompletableFuture<RequestVoteReply> future = harness.sendRequestVote(PEER, new RequestVoteArgs(1, NODE, 0, 0));
        Thread.sleep(100);
        assertFalse(future.isDone());
        assertEquals(0, delegate.requestVotes.get());
        harness.close();
    }

    @Test
    void delayedMessageEventuallyDelivered() throws Exception {
        RecordingTransport delegate = new RecordingTransport();
        ChaosRules rules = new ChaosRules();
        rules.delayLink(NODE, PEER, 150);
        ChaosTransportClient harness = harness(delegate, rules);

        CompletableFuture<RequestVoteReply> future = harness.sendRequestVote(PEER, new RequestVoteArgs(1, NODE, 0, 0));
        assertFalse(future.isDone());

        RequestVoteReply reply = future.get(3, TimeUnit.SECONDS);
        assertTrue(reply.isVoteGranted());
        assertEquals(1, delegate.requestVotes.get());
        harness.close();
    }
}
