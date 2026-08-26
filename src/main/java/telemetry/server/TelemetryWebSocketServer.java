package telemetry.server;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import telemetry.TelemetryBus;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WebSocket server that streams Raft telemetry to dashboard clients. Each
 * connected client is wrapped in a {@link WebSocketSubscriber} and subscribed
 * to the shared {@link TelemetryBus}; on disconnect it is unsubscribed.
 */
public class TelemetryWebSocketServer extends WebSocketServer {

    private static final Logger LOG = Logger.getLogger(TelemetryWebSocketServer.class.getName());

    private final TelemetryBus bus;
    private final Map<WebSocket, WebSocketSubscriber> dashboardClients = new ConcurrentHashMap<>();

    public TelemetryWebSocketServer(TelemetryBus bus, int port) {
        super(new InetSocketAddress(port));
        this.bus = bus;
        setReuseAddr(true);
        // Don't let the Java-WebSocket idle/ping watchdog drop healthy dashboard
        // connections between events — the app reconnects anyway.
        setConnectionLostTimeout(0);
    }

    @Override
    public void onStart() {
        // startup logging handled by Java-WebSocket
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        WebSocketSubscriber subscriber = new WebSocketSubscriber(conn);
        dashboardClients.put(conn, subscriber);
        bus.subscribe(subscriber);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        WebSocketSubscriber subscriber = dashboardClients.remove(conn);
        if (subscriber != null) {
            bus.unsubscribe(subscriber);
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Telemetry WebSocket error", ex);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // dashboards are receive-only in this build
    }

    public int getConnectedDashboardCount() {
        return dashboardClients.size();
    }
}
