package consensus_core.state;

public class VolatileState {
    private long commitIndex = 0;
    private long lastApplied = 0;

    public long getCommitIndex() { return commitIndex; }
    public void setCommitIndex(long commitIndex) { this.commitIndex = commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public void setLastApplied(long lastApplied) { this.lastApplied = lastApplied; }
}
