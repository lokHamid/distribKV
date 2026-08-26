package kv_store.command;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KVCommandCodecTest {

    @Test
    void putWithValueRoundTrips() {
        KVCommand original = new KVCommand(CommandType.PUT, "k", "v");
        byte[] encoded = KVCommandCodec.encode(original);
        KVCommand decoded = KVCommandCodec.decode(encoded);
        assertEquals(CommandType.PUT, decoded.getType());
        assertEquals("k", decoded.getKey());
        assertEquals("v", decoded.getValue());
    }

    @Test
    void getWithNullValueRoundTrips() {
        KVCommand original = new KVCommand(CommandType.GET, "k", null);
        KVCommand decoded = KVCommandCodec.decode(KVCommandCodec.encode(original));
        assertEquals(CommandType.GET, decoded.getType());
        assertEquals("k", decoded.getKey());
        assertNull(decoded.getValue());
    }

    @Test
    void headersAreReadBack() {
        for (CommandType type : CommandType.values()) {
            KVCommand decoded = KVCommandCodec.decode(KVCommandCodec.encode(new KVCommand(type, "K", "V")));
            assertEquals(type, decoded.getType());
        }
    }
}
