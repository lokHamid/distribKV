// kv_store/result/KVResult.java
package kv_store.result;

public final class KVResult {
    private final boolean success;
    private final String value;      // populated for successful GET
    private final String errorMessage;

    private KVResult(boolean success, String value, String errorMessage) {
        this.success = success;
        this.value = value;
        this.errorMessage = errorMessage;
    }

    public static KVResult ok(String value) { return new KVResult(true, value, null); }
    public static KVResult error(String message) { return new KVResult(false, null, message); }

    public boolean isSuccess() { return success; }
    public String getValue() { return value; }
    public String getErrorMessage() { return errorMessage; }
}