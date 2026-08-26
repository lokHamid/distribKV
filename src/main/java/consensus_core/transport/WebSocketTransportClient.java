package consensus_core.transport;

import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.enums.ReadyState;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link TransportClient} implementation that talks to peer Raft nodes over
 * WebSocket. One persistent connection per peer; requests/replies are
 * multiplexed by a monotonic correlation id.
 *
 * <p>The peer connection is evicted on close/error so the next send transparently
 * reconnects (a crashed peer must not poison a permanently-cached dead socket),
 * and any in-flight RPC to that peer is failed so the caller re-elects/retries.
 */
public class WebSocketTransportClient implements TransportClient {

    private final Map<String, WebSocketClient> connections = new ConcurrentHashMap<>();
    private final Map<String, String> peerAddresses;
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private final AtomicLong correlationId = new AtomicLong(0);

    public WebSocketTransportClient(Map<String, String> peerAddresses) {
        this.peerAddresses = peerAddresses;
    }

    private record Pending(String peer, CompletableFuture<?> future) {
    }

    @Override
    public CompletableFuture<RequestVoteReply> sendRequestVote(String targetNodeId, RequestVoteArgs args) {
        CompletableFuture<RequestVoteReply> future = new CompletableFuture<>();
        long id = register(targetNodeId, future);
        try {
            sendFrame(targetNodeId, id, RaftWireCodec.requestVoteFrame(id, args));
        } catch (RuntimeException e) {
            fail(id, e);
        }
        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesReply> sendAppendEntries(String targetNodeId, AppendEntriesArgs args) {
        CompletableFuture<AppendEntriesReply> future = new CompletableFuture<>();
        long id = register(targetNodeId, future);
        try {
            sendFrame(targetNodeId, id, RaftWireCodec.appendEntriesFrame(id, args));
        } catch (RuntimeException e) {
            fail(id, e);
        }
        return future;
    }

    @Override
    public CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String targetNodeId, InstallSnapshotArgs args) {
        CompletableFuture<InstallSnapshotReply> future = new CompletableFuture<>();
        long id = register(targetNodeId, future);
        try {
            sendFrame(targetNodeId, id, RaftWireCodec.installSnapshotFrame(id, args));
        } catch (RuntimeException e) {
            fail(id, e);
        }
        return future;
    }

    @SuppressWarnings("unchecked")
    private <T> long register(String targetNodeId, CompletableFuture<T> future) {
        long id = correlationId.incrementAndGet();
        pending.put(id, new Pending(targetNodeId, future));
        return id;
    }

    private void fail(long id, RuntimeException error) {
        Pending removed = pending.remove(id);
        if (removed != null) {
            ((CompletableFuture<Object>) removed.future()).completeExceptionally(error);
        }
    }

    private void sendFrame(String targetNodeId, long id, byte[] frame) {
        WebSocketClient client = getOrConnect(targetNodeId);
        if (client.getReadyState() == ReadyState.OPEN) {
            client.send(frame);
        } else if (client.getReadyState() == ReadyState.NOT_YET_CONNECTED) {
            // connect() is ongoing; queue the frame and flush it on onOpen.
            ((PeerSocket) client).enqueue(frame);
        } else {
            throw new IllegalStateException("peer connection " + targetNodeId
                    + " not available: " + client.getReadyState());
        }
    }

    private WebSocketClient getOrConnect(String nodeId) {
        return connections.computeIfAbsent(nodeId, id -> {
            String address = peerAddresses.get(id);
            if (address == null) {
                throw new IllegalStateException("No address configured for peer " + id);
            }
            return new PeerSocket(URI.create(address), id);
        });
    }

    private class PeerSocket extends WebSocketClient {
        private final String peer;
        private final List<byte[]> backlog = new CopyOnWriteArrayList<>();

        PeerSocket(URI serverUri, String peer) {
            super(serverUri);
            this.peer = peer;
            connect();
        }

        void enqueue(byte[] frame) {
            if (getReadyState() == ReadyState.OPEN) {
                send(frame);
            } else {
                backlog.add(frame);
            }
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            for (byte[] frame : backlog) {
                send(frame);
            }
            backlog.clear();
        }

        @Override
        public void onMessage(String message) {
        }

        @Override
        public void onMessage(ByteBuffer message) {
            byte[] data = new byte[message.remaining()];
            message.get(data);
            handleReply(data);
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            evict(peer, new IllegalStateException("peer " + peer + " disconnected"));
        }

        @Override
        public void onError(Exception ex) {
            evict(peer, new IllegalStateException("peer " + peer + " transport error: " + ex, ex));
        }
    }

    private void evict(String peer, RuntimeException cause) {
        connections.remove(peer);
        Iterator<Map.Entry<Long, Pending>> it = pending.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Pending> entry = it.next();
            if (entry.getValue().peer().equals(peer)) {
                it.remove();
                ((CompletableFuture<Object>) (Object) entry.getValue().future()).completeExceptionally(cause);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void handleReply(byte[] message) {
        RaftWireCodec.Frame frame = RaftWireCodec.parseFrame(message);
        Object reply = switch (frame.type()) {
            case RaftWireCodec.TYPE_REQUEST_VOTE_REPLY ->
                    RaftWireCodec.decodeRequestVoteReply(frame.payload());
            case RaftWireCodec.TYPE_APPEND_ENTRIES_REPLY ->
                    RaftWireCodec.decodeAppendEntriesReply(frame.payload());
            case RaftWireCodec.TYPE_INSTALL_SNAPSHOT_REPLY ->
                    RaftWireCodec.decodeInstallSnapshotReply(frame.payload());
            default -> null;
        };
        Pending p = pending.remove(frame.correlationId());
        if (p != null && reply != null) {
            ((CompletableFuture) p.future()).complete(reply);
        }
    }

    public void close() {
        for (WebSocketClient client : connections.values()) {
            client.close();
        }
        connections.clear();
        for (Pending p : pending.values()) {
            p.future().cancel(true);
        }
        pending.clear();
    }
}
