package consensus_core.rpc;

public final class InstallSnapshotReply {
    private final long term;
    private final boolean success;

    public InstallSnapshotReply(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public long getTerm() { return term; }
    public boolean isSuccess() { return success; }
}
