package consensus_core.transport;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Inbound WebSocket server that accepts Raft RPCs from peers and dispatches
 * them to a {@link RaftRpcHandler} (typically a RaftNode), then replies over
 * the same connection using the correlation id to correlate the response.
 */
public class RaftRpcServer extends WebSocketServer {

    private static final Logger LOG = Logger.getLogger(RaftRpcServer.class.getName());

    private final RaftRpcHandler handler;

    public RaftRpcServer(RaftRpcHandler handler, int port) {
        super(new InetSocketAddress(port));
        this.handler = handler;
        setReuseAddr(true);
    }

    public RaftRpcServer(RaftRpcHandler handler, InetSocketAddress address) {
        super(address);
        this.handler = handler;
        setReuseAddr(true);
    }

    /** Convenience so a node can retrieve its bind address (e.g. port 0). */
    public InetSocketAddress boundAddress() {
        return getAddress();
    }

    @Override
    public void onStart() {
        // startup logging handled by Java-WebSocket
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        // no meaningful handshake headers required for RPC
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        // peer went away; nothing to clean up inside the server
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "WebSocket RPC server error", ex);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // RPC is binary; text frames are ignored to keep the protocol strict.
        conn.send(message.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer message) {
        byte[] data = new byte[message.remaining()];
        message.get(data);
        RaftWireCodec.Frame frame = RaftWireCodec.parseFrame(data);
        byte[] reply = switch (frame.type()) {
            case RaftWireCodec.TYPE_REQUEST_VOTE -> RaftWireCodec.requestVoteReplyFrame(
                    frame.correlationId(), handler.handleRequestVote(RaftWireCodec.decodeRequestVote(frame.payload())));
            case RaftWireCodec.TYPE_APPEND_ENTRIES -> RaftWireCodec.appendEntriesReplyFrame(
                    frame.correlationId(), handler.handleAppendEntries(RaftWireCodec.decodeAppendEntries(frame.payload())));
            case RaftWireCodec.TYPE_INSTALL_SNAPSHOT -> RaftWireCodec.installSnapshotReplyFrame(
                    frame.correlationId(), handler.handleInstallSnapshot(RaftWireCodec.decodeInstallSnapshot(frame.payload())));
            default -> null;
        };
        if (reply != null && conn.isOpen()) {
            conn.send(reply);
        }
    }
}
