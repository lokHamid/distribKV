package kv_store;

import kv_store.command.CommandType;
import kv_store.command.KVCommand;
import kv_store.command.KVCommandCodec;
import kv_store.result.KVResult;
import kv_store.result.KVResultCodec;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KVStateMachineTest {

    private final KVStateMachine sm = new KVStateMachine(new KVStore());

    private KVResult apply(KVCommand command) {
        return KVResultCodec.decode(sm.apply(KVCommandCodec.encode(command)));
    }

    @Test
    void putThenGetReturnsValue() {
        assertTrue(apply(new KVCommand(CommandType.PUT, "k", "v")).isSuccess());
        KVResult result = apply(new KVCommand(CommandType.GET, "k", null));
        assertTrue(result.isSuccess());
        assertEquals("v", result.getValue());
    }

    @Test
    void getMissingKeyReturnsError() {
        KVResult result = apply(new KVCommand(CommandType.GET, "absent", null));
        assertFalse(result.isSuccess());
        assertEquals("key not found", result.getErrorMessage());
    }

    @Test
    void deleteRemovesKey() {
        apply(new KVCommand(CommandType.PUT, "k", "v"));
        assertTrue(apply(new KVCommand(CommandType.DELETE, "k", null)).isSuccess());
        assertFalse(apply(new KVCommand(CommandType.GET, "k", null)).isSuccess());
    }

    @Test
    void installSnapshotReplacesState() {
        apply(new KVCommand(CommandType.PUT, "old", "value"));
        // build a snapshot with only the new key
        sm.installSnapshot(kv_store.snapshot.KVSnapshotCodec.encode(java.util.Map.of("new", "state")));
        assertFalse(apply(new KVCommand(CommandType.GET, "old", null)).isSuccess());
        assertEquals("state", apply(new KVCommand(CommandType.GET, "new", null)).getValue());
    }
}
