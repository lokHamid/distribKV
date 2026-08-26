// telemetry/event/RaftEventCodec.java
package telemetry.event;

import consensus_core.api.RaftEvent;

/**
 * Serializes RaftEvent to JSON for transmission to the JavaFX dashboard
 * over WebSocket/SSE. Keep this hand-rolled or swap in Jackson/Gson later —
 * shown minimal here so there's no hidden dependency assumption.
 */
public final class RaftEventCodec {

    public static String toJson(RaftEvent event) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"").append(event.getType()).append("\",");
        sb.append("\"nodeId\":\"").append(event.getNodeId()).append("\",");
        sb.append("\"term\":").append(event.getTerm()).append(",");
        sb.append("\"timestamp\":").append(event.getTimestamp()).append(",");
        sb.append("\"metadata\":{");
        boolean first = true;
        for (var entry : event.getMetadata().entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }
}