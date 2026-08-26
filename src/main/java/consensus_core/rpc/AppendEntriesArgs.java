package consensus_core.rpc;

import consensus_core.log.LogEntry;

import java.util.Collections;
import java.util.List;

public final class AppendEntriesArgs {
    private final long term;
    private final String leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<LogEntry> entries;
    private final long leaderCommit;

    public AppendEntriesArgs(long term, String leaderId, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? Collections.unmodifiableList(entries) : Collections.emptyList();
        this.leaderCommit = leaderCommit;
    }

    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public List<LogEntry> getEntries() { return entries; }
    public long getLeaderCommit() { return leaderCommit; }
}
