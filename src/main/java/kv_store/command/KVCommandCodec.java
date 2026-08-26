// kv_store/command/KVCommandCodec.java
package kv_store.command;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Encodes/decodes KVCommand to the byte[] payload stored in LogEntry.command.
 * Format: [typeOrdinal:1][keyLen:4][keyBytes][hasValue:1][valueLen:4][valueBytes]
 */
public final class KVCommandCodec {

    public static byte[] encode(KVCommand cmd) {
        byte[] keyBytes = cmd.getKey().getBytes(StandardCharsets.UTF_8);
        boolean hasValue = cmd.getValue() != null;
        byte[] valueBytes = hasValue ? cmd.getValue().getBytes(StandardCharsets.UTF_8) : new byte[0];

        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + keyBytes.length + 1 + 4 + valueBytes.length);
        buf.put((byte) cmd.getType().ordinal());
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.put((byte) (hasValue ? 1 : 0));
        buf.putInt(valueBytes.length);
        buf.put(valueBytes);
        return buf.array();
    }

    public static KVCommand decode(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        CommandType type = CommandType.values()[buf.get()];

        int keyLen = buf.getInt();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);

        boolean hasValue = buf.get() == 1;
        int valueLen = buf.getInt();
        byte[] valueBytes = new byte[valueLen];
        buf.get(valueBytes);

        String key = new String(keyBytes, StandardCharsets.UTF_8);
        String value = hasValue ? new String(valueBytes, StandardCharsets.UTF_8) : null;
        return new KVCommand(type, key, value);
    }
}