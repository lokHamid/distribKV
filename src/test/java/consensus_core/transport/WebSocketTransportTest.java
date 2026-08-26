package consensus_core.transport;

import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class WebSocketTransportTest {

    private static RaftRpcServer server;

    @AfterAll
    static void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static RaftRpcServer startServer(RaftRpcHandler handler) {
        try {
            server = new RaftRpcServer(handler, freePort());
            server.start();
            // give the server thread a moment to begin accepting
            Thread.sleep(200);
            return server;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void requestVoteRoundTripsOverSocket() throws Exception {
        RaftRpcServer srv = startServer(new RaftRpcHandler() {
            @Override
            public RequestVoteReply handleRequestVote(RequestVoteArgs a) {
                return new RequestVoteReply(a.getTerm(), true);
            }
            @Override
            public AppendEntriesReply handleAppendEntries(AppendEntriesArgs a) {
                return new AppendEntriesReply(a.getTerm(), true, a.getPrevLogIndex());
            }
            @Override
            public InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs a) {
                return new InstallSnapshotReply(a.getTerm(), true);
            }
        });

        WebSocketTransportClient client = new WebSocketTransportClient(Map.of(
                "node-2", "ws://127.0.0.1:" + srv.boundAddress().getPort()));
        try {
            RequestVoteReply reply = client.sendRequestVote("node-2", new RequestVoteArgs(4, "node-1", 1, 1))
                    .get(5, TimeUnit.SECONDS);
            assertTrue(reply.isVoteGranted());
            assertEquals(4, reply.getTerm());
        } finally {
            client.close();
        }
    }

    @Test
    void appendEntriesRoundTripsOverSocket() throws Exception {
        RaftRpcServer srv = startServer(new RaftRpcHandler() {
            @Override
            public RequestVoteReply handleRequestVote(RequestVoteArgs a) {
                return new RequestVoteReply(a.getTerm(), true);
            }
            @Override
            public AppendEntriesReply handleAppendEntries(AppendEntriesArgs a) {
                return new AppendEntriesReply(a.getTerm(), true, 7);
            }
            @Override
            public InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs a) {
                return new InstallSnapshotReply(a.getTerm(), true);
            }
        });

        WebSocketTransportClient client = new WebSocketTransportClient(Map.of(
                "node-2", "ws://127.0.0.1:" + srv.boundAddress().getPort()));
        try {
            AppendEntriesReply reply = client.sendAppendEntries("node-2",
                            new AppendEntriesArgs(2, "node-1", 6, 1, java.util.List.of(), 5))
                    .get(5, TimeUnit.SECONDS);
            assertTrue(reply.isSuccess());
            assertEquals(7, reply.getMatchIndex());
        } finally {
            client.close();
        }
    }
}
