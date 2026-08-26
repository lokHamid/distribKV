// kv_store/KVStateMachine.java
package kv_store;

import consensus_core.api.StateMachine;
import consensus_core.api.SnapshotProvider;
import kv_store.command.KVCommand;
import kv_store.command.KVCommandCodec;
import kv_store.result.KVResult;
import kv_store.result.KVResultCodec;
import kv_store.snapshot.KVSnapshotCodec;

/**
 * The only bridge between consensus_core and kv_store.
 * RaftNode calls apply() once an entry is committed (majority-replicated).
 */
public class KVStateMachine implements StateMachine, SnapshotProvider {
    private final KVStore store;

    public KVStateMachine(KVStore store) {
        this.store = store;
    }

    @Override
    public byte[] apply(byte[] command) {
        KVCommand cmd = KVCommandCodec.decode(command);
        KVResult result = switch (cmd.getType()) {
            case PUT -> {
                store.put(cmd.getKey(), cmd.getValue());
                yield KVResult.ok(null);
            }
            case DELETE -> {
                store.delete(cmd.getKey());
                yield KVResult.ok(null);
            }
            case GET -> {
                String value = store.get(cmd.getKey());
                yield value != null ? KVResult.ok(value) : KVResult.error("key not found");
            }
        };
        return KVResultCodec.encode(result);
    }

    @Override
    public void installSnapshot(byte[] snapshotData) {
        store.restoreFrom(KVSnapshotCodec.decode(snapshotData));
    }

    @Override
    public byte[] createSnapshot() {
        return KVSnapshotCodec.encode(store.snapshotView());
    }

    /** Convenience accessor for reading the underlying store (e.g. tests, snapshot capture). */
    public KVStore getStore() {
        return store;
    }
}