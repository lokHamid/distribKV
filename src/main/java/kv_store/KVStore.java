// kv_store/KVStore.java
package kv_store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Pure in-memory storage. No knowledge of Raft, commands, or byte encoding.
 * This is what actually gets mutated once consensus has already decided
 * an operation is committed.
 */
public class KVStore {
    private final Map<String, String> data = new ConcurrentHashMap<>();

    public void put(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }

    public void delete(String key) {
        data.remove(key);
    }

    /** Used by KVSnapshotCodec to capture full state. */
    public Map<String, String> snapshotView() {
        return Map.copyOf(data);
    }

    /** Used when installing a snapshot — replaces all current state. */
    public void restoreFrom(Map<String, String> snapshotData) {
        data.clear();
        data.putAll(snapshotData);
    }
}