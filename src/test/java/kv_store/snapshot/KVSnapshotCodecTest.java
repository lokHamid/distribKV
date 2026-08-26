package kv_store.snapshot;

import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class KVSnapshotCodecTest {

    @Test
    void emptyMapRoundTrips() {
        Map<String, String> decoded = KVSnapshotCodec.decode(KVSnapshotCodec.encode(Map.of()));
        assertTrue(decoded.isEmpty());
    }

    @Test
    void populatedMapRoundTrips() {
        Map<String, String> original = new LinkedHashMap<>();
        original.put("a", "1");
        original.put("b", "two");
        original.put("longer-key", "a longer value with spaces");
        Map<String, String> decoded = KVSnapshotCodec.decode(KVSnapshotCodec.encode(original));
        assertEquals(original, decoded);
    }
}
