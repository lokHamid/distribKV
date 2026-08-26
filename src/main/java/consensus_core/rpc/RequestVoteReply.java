package consensus_core.rpc;

public final class RequestVoteReply {
    private final long term;
    private final boolean voteGranted;

    public RequestVoteReply(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}
