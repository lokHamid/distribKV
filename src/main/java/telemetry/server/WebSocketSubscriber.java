// telemetry/server/WebSocketSubscriber.java
package telemetry.server;

import consensus_core.api.RaftEvent;
import org.java_websocket.WebSocket;
import telemetry.event.RaftEventCodec;
import telemetry.subscriber.TelemetrySubscriber;

/**
 * Wraps one live dashboard WebSocket connection as a TelemetrySubscriber.
 * When RaftNode publishes an event, TelemetryBus calls onEvent() here, which
 * pushes the JSON straight down the socket to the connected dashboard client.
 */
public class WebSocketSubscriber implements TelemetrySubscriber {
    private final WebSocket session;

    public WebSocketSubscriber(WebSocket session) {
        this.session = session;
    }

    @Override
    public void onEvent(RaftEvent event) {
        if (session.isOpen()) {
            session.send(RaftEventCodec.toJson(event));
        }
    }
}
