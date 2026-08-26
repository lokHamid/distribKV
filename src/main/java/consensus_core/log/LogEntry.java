package consensus_core.log;

public class LogEntry {
    private final long index;
    private final long term;
    private final byte[] command;

    public LogEntry(long index, long term, byte[] command) {
        this.index = index;
        this.term = term;
        this.command = command;
    }

    public long getIndex() { return index; }
    public long getTerm() { return term; }
    public byte[] getCommand() { return command; }
}
