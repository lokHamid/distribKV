// kv_store/snapshot/KVSnapshotCodec.java
package kv_store.snapshot;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/** Encodes/decodes the full KVStore map for StateMachine.installSnapshot(). */
public final class KVSnapshotCodec {

    public static byte[] encode(Map<String, String> data) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : data.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode snapshot", e);
        }
    }

    public static Map<String, String> decode(byte[] snapshotData) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshotData))) {
            int size = in.readInt();
            Map<String, String> result = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                result.put(key, value);
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode snapshot", e);
        }
    }
}