package consensus_core.rpc;

public final class InstallSnapshotArgs {
    private final long term;
    private final String leaderId;
    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final byte[] data;

    public InstallSnapshotArgs(long term, String leaderId, long lastIncludedIndex,
                               long lastIncludedTerm, byte[] data) {
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.data = data;
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getLastIncludedIndex() { return lastIncludedIndex; }
    public long getLastIncludedTerm() { return lastIncludedTerm; }
    public byte[] getData() { return data; }
}
