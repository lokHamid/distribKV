package telemetry.event;

import consensus_core.api.EventType;
import consensus_core.api.RaftEvent;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class RaftEventCodecTest {

    @Test
    void encodesExpectedFields() {
        RaftEvent event = new RaftEvent(EventType.LEADER_ELECTED, "node-3", 7, 123456789L,
                Map.of("leaderId", "node-3"));
        String json = RaftEventCodec.toJson(event);
        assertTrue(json.startsWith("{"));
        assertTrue(json.endsWith("}"));
        assertTrue(json.contains("\"type\":\"LEADER_ELECTED\""));
        assertTrue(json.contains("\"nodeId\":\"node-3\""));
        assertTrue(json.contains("\"term\":7"));
        assertTrue(json.contains("\"timestamp\":123456789"));
        assertTrue(json.contains("\"leaderId\":\"node-3\""));
    }

    @Test
    void emptyMetadataProducesEmptyObject() {
        RaftEvent event = new RaftEvent(EventType.NODE_STARTED, "n", 0, 1L, Map.of());
        assertTrue(RaftEventCodec.toJson(event).contains("\"metadata\":{}"));
    }
}
