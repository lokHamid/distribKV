// kv_store/result/KVResultCodec.java
package kv_store.result;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Encodes/decodes a KVResult into the byte[] returned to the caller after a
 * committed command is applied. Mirrors the layout style of KVCommandCodec.
 *
 * Format: [success:1][hasValue:1][valueLen:4][valueBytes][errorLen:4][errorBytes]
 */
public final class KVResultCodec {

    private KVResultCodec() {
    }

    public static byte[] encode(KVResult result) {
        byte[] valueBytes = result.getValue() != null
                ? result.getValue().getBytes(StandardCharsets.UTF_8)
                : new byte[0];
        byte[] errorBytes = result.getErrorMessage() != null
                ? result.getErrorMessage().getBytes(StandardCharsets.UTF_8)
                : new byte[0];

        ByteBuffer buf = ByteBuffer.allocate(1 + 1 + 4 + valueBytes.length + 4 + errorBytes.length);
        buf.put((byte) (result.isSuccess() ? 1 : 0));
        buf.put((byte) (result.getValue() != null ? 1 : 0));
        buf.putInt(valueBytes.length);
        buf.put(valueBytes);
        buf.putInt(errorBytes.length);
        buf.put(errorBytes);
        return buf.array();
    }

    public static KVResult decode(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        boolean success = buf.get() == 1;
        boolean hasValue = buf.get() == 1;

        int valueLen = buf.getInt();
        byte[] valueBytes = new byte[valueLen];
        buf.get(valueBytes);

        int errorLen = buf.getInt();
        byte[] errorBytes = new byte[errorLen];
        buf.get(errorBytes);

        String value = hasValue ? new String(valueBytes, StandardCharsets.UTF_8) : null;
        String error = success ? null : new String(errorBytes, StandardCharsets.UTF_8);
        return success ? KVResult.ok(value) : KVResult.error(error);
    }
}
