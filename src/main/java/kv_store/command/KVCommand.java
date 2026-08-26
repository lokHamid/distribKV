package kv_store.command;

public final class KVCommand {
    private final CommandType type;
    private final String key;
    private final String value; // null for GET/DELETE

    public KVCommand(CommandType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public CommandType getType() { return type; }
    public String getKey() { return key; }
    public String getValue() { return value; }
}