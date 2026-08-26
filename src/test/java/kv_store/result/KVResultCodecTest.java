package kv_store.result;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KVResultCodecTest {

    @Test
    void successWithValueRoundTrips() {
        KVResult decoded = KVResultCodec.decode(KVResultCodec.encode(KVResult.ok("foo")));
        assertTrue(decoded.isSuccess());
        assertEquals("foo", decoded.getValue());
        assertNull(decoded.getErrorMessage());
    }

    @Test
    void successWithoutValueRoundTrips() {
        KVResult decoded = KVResultCodec.decode(KVResultCodec.encode(KVResult.ok(null)));
        assertTrue(decoded.isSuccess());
        assertNull(decoded.getValue());
        assertNull(decoded.getErrorMessage());
    }

    @Test
    void errorRoundTrips() {
        KVResult decoded = KVResultCodec.decode(KVResultCodec.encode(KVResult.error("key not found")));
        assertFalse(decoded.isSuccess());
        assertNull(decoded.getValue());
        assertEquals("key not found", decoded.getErrorMessage());
    }
}
