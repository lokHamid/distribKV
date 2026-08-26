package consensus_core.node;

import consensus_core.api.StateMachine;
import consensus_core.api.SnapshotProvider;
import consensus_core.api.EventType;
import consensus_core.api.RaftEvent;
import consensus_core.api.TelemetryPublisher;
import consensus_core.log.LogEntry;
import consensus_core.log.RaftLog;
import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import consensus_core.state.PersistentState;
import consensus_core.state.VolatileState;
import consensus_core.transport.RaftRpcHandler;
import consensus_core.transport.TransportClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A single Raft node. Implements leader election, log replication, the safety
 * properties and the RPC handlers. Concurrency is coarse-grained on the node
 * object to keep the invariants easy to reason about; timers and submitter
 * threads funnel into synchronized entrypoints.
 */
public class RaftNode implements RaftRpcHandler {

    private final RaftConfig config;
    private final PersistentState persistentState;
    private final VolatileState volatileState;
    private final RaftLog log;
    private final StateMachine stateMachine;
    private final TransportClient transport;
    private final TelemetryPublisher telemetry;

    private NodeRole role = NodeRole.FOLLOWER;
    private String currentLeaderId = null;
    private final Set<String> members = new HashSet<>();
    private final Set<String> votesReceived = new HashSet<>();
    private int majority;

    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    // in-memory view of the last snapshot so it can be shipped to lagging followers
    private volatile byte[] lastSnapshotData = null;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;
    private volatile boolean running = false;

    public RaftNode(RaftConfig config, PersistentState persistentState, VolatileState volatileState,
                    RaftLog log, StateMachine stateMachine, TransportClient transport, TelemetryPublisher telemetry) {
        this.config = config;
        this.persistentState = persistentState;
        this.volatileState = volatileState;
        this.log = log;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.telemetry = telemetry;

        this.members.add(config.getNodeId());
        this.members.addAll(config.getPeerIds());
        this.members.removeIf(String::isBlank);
        this.majority = members.size() / 2 + 1;
    }

    // ------------------------------------------------------------------ lifecycle

    /** Loads durable state/log and starts the role timers. */
    public synchronized void start() {
        persistentState.load();
        log.load();
        running = true;
        publish(EventType.NODE_STARTED, null);
        resetElectionTimeout();
    }

    public synchronized void stop() {
        running = false;
        cancelElectionTimeout();
        cancelHeartbeat();
        scheduler.shutdownNow();
    }

    // ------------------------------------------------------------------ timers

    private void resetElectionTimeout() {
        cancelElectionTimeout();
        long range = Math.max(1, config.getMaxElectionTimeoutMs() - config.getMinElectionTimeoutMs());
        long timeout = config.getMinElectionTimeoutMs() + ThreadLocalRandom.current().nextLong(range);
        electionTimeoutTask = scheduler.schedule(this::runElectionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    private void runElectionTimeout() {
        synchronized (RaftNode.this) {
            if (!running) {
                return;
            }
            startElection();
        }
    }

    private void cancelElectionTimeout() {
        if (electionTimeoutTask != null) {
            electionTimeoutTask.cancel(false);
            electionTimeoutTask = null;
        }
    }

    private void startHeartbeat() {
        cancelHeartbeat();
        if (config.getHeartbeatIntervalMs() > 0) {
            heartbeatTask = scheduler.scheduleAtFixedRate(this::runHeartbeat,
                    config.getHeartbeatIntervalMs(), config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void runHeartbeat() {
        synchronized (RaftNode.this) {
            if (!running || role != NodeRole.LEADER) {
                return;
            }
            heartbeat();
        }
    }

    private void cancelHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
    }

    // ------------------------------------------------------------------ role transitions

    private void becomeFollower(long term, String reason) {
        if (role == NodeRole.LEADER) {
            publish(EventType.LEADER_LOST, null);
        }
        boolean termChanged = term > persistentState.getCurrentTerm();
        if (termChanged) {
            persistentState.setCurrentTerm(term);
            persistentState.setVotedFor(null); // no vote cast yet in the new term
            publish(EventType.TERM_CHANGED, Map.of("term", Long.toString(term)));
        }
        NodeRole from = role;
        role = NodeRole.FOLLOWER;
        currentLeaderId = null;
        votesReceived.clear();
        cancelHeartbeat();
        resetElectionTimeout();
        if (from != NodeRole.FOLLOWER) {
            publishRoleChange(from, NodeRole.FOLLOWER);
        }
    }

    private void becomeCandidate() {
        NodeRole from = role;
        long newTerm = persistentState.getCurrentTerm() + 1;
        persistentState.setCurrentTerm(newTerm);
        persistentState.setVotedFor(config.getNodeId());
        publish(EventType.TERM_CHANGED, Map.of("term", Long.toString(newTerm)));
        role = NodeRole.CANDIDATE;
        currentLeaderId = null;
        votesReceived.clear();
        votesReceived.add(config.getNodeId());
        publishRoleChange(from, NodeRole.CANDIDATE);
        cancelHeartbeat();
        resetElectionTimeout();
    }

    private void becomeLeader() {
        NodeRole from = role;
        role = NodeRole.LEADER;
        currentLeaderId = config.getNodeId();
        publishRoleChange(from, NodeRole.LEADER);
        publish(EventType.LEADER_ELECTED, Map.of("leaderId", config.getNodeId()));
        cancelElectionTimeout();
        long lastIndex = log.getLastIndex();
        for (String peer : members) {
            if (peer.equals(config.getNodeId())) {
                continue;
            }
            nextIndex.put(peer, lastIndex + 1);
            matchIndex.put(peer, 0L);
        }
        startHeartbeat();
        heartbeat(); // send initial keepalives and any backlog immediately
    }

    private void publishRoleChange(NodeRole from, NodeRole to) {
        publish(EventType.ROLE_CHANGED, Map.of("from", from.name(), "to", to.name()));
    }

    // ------------------------------------------------------------------ election

    private void startElection() {
        becomeCandidate();
        if (votesReceived.size() >= majority) {
            becomeLeader();
            return;
        }
        long lastLogIndex = log.getLastIndex();
        long lastLogTerm = log.getLastTerm();
        RequestVoteArgs args = new RequestVoteArgs(persistentState.getCurrentTerm(), config.getNodeId(), lastLogIndex, lastLogTerm);
        for (String peer : members) {
            if (peer.equals(config.getNodeId())) {
                continue;
            }
            transport.sendRequestVote(peer, args).thenAccept(reply ->
                    handleAsync(() -> handleVoteReply(peer, reply)));
        }
    }

    private void handleVoteReply(String peer, RequestVoteReply reply) {
        if (role != NodeRole.CANDIDATE) {
            return;
        }
        if (reply.getTerm() > persistentState.getCurrentTerm()) {
            becomeFollower(reply.getTerm(), "higher term in vote reply");
            return;
        }
        if (reply.getTerm() < persistentState.getCurrentTerm()) {
            return; // stale reply
        }
        if (reply.isVoteGranted()) {
            votesReceived.add(peer);
            if (votesReceived.size() >= majority) {
                becomeLeader();
            }
        }
    }

    // ------------------------------------------------------------------ RPC handlers (server side)

    @Override
    public synchronized RequestVoteReply handleRequestVote(RequestVoteArgs args) {
        if (args.getTerm() < persistentState.getCurrentTerm()) {
            publish(EventType.VOTE_DENIED, Map.of("candidateId", args.getCandidateId()));
            return new RequestVoteReply(persistentState.getCurrentTerm(), false);
        }
        if (args.getTerm() > persistentState.getCurrentTerm()) {
            becomeFollower(args.getTerm(), "saw higher term");
        }
        long lastLogIndex = log.getLastIndex();
        long lastLogTerm = log.getLastTerm();
        boolean candidateUpToDate = args.getLastLogTerm() > lastLogTerm
                || (args.getLastLogTerm() == lastLogTerm && args.getLastLogIndex() >= lastLogIndex);
        String votedFor = persistentState.getVotedFor();
        boolean canVote = (votedFor == null || votedFor.equals(args.getCandidateId())) && candidateUpToDate;
        if (canVote) {
            persistentState.setVotedFor(args.getCandidateId());
            resetElectionTimeout();
            publish(EventType.VOTE_GRANTED, Map.of("votedFor", args.getCandidateId()));
            return new RequestVoteReply(persistentState.getCurrentTerm(), true);
        }
        publish(EventType.VOTE_DENIED, Map.of("candidateId", args.getCandidateId()));
        return new RequestVoteReply(persistentState.getCurrentTerm(), false);
    }

    @Override
    public synchronized AppendEntriesReply handleAppendEntries(AppendEntriesArgs args) {
        if (args.getTerm() < persistentState.getCurrentTerm()) {
            return new AppendEntriesReply(persistentState.getCurrentTerm(), false, log.getLastIndex());
        }
        if (args.getTerm() > persistentState.getCurrentTerm()) {
            becomeFollower(args.getTerm(), "saw higher term");
        } else if (role != NodeRole.FOLLOWER) {
            becomeFollower(args.getTerm(), "leader contact");
        }
        currentLeaderId = args.getLeaderId();
        resetElectionTimeout();

        // consistency check
        if (args.getPrevLogIndex() != 0 && log.getTermAt(args.getPrevLogIndex()) != args.getPrevLogTerm()) {
            publish(EventType.HEARTBEAT_RECEIVED, Map.of("leaderId", args.getLeaderId()));
            return new AppendEntriesReply(persistentState.getCurrentTerm(), false, log.getLastIndex());
        }

        List<LogEntry> incoming = args.getEntries();
        if (!incoming.isEmpty()) {
            for (int i = 0; i < incoming.size(); i++) {
                LogEntry entry = incoming.get(i);
                long existingTerm = log.getTermAt(entry.getIndex());
                if (existingTerm != entry.getTerm()) {
                    log.truncateSuffix(entry.getIndex() - 1);
                    log.append(incoming.subList(i, incoming.size()));
                    break;
                }
            }
        }

        // advance commit as far as the leader has committed
        if (args.getLeaderCommit() > volatileState.getCommitIndex()) {
            volatileState.setCommitIndex(Math.min(args.getLeaderCommit(), log.getLastIndex()));
            applyCommitted();
        }
        publish(EventType.HEARTBEAT_RECEIVED, Map.of("leaderId", args.getLeaderId()));
        return new AppendEntriesReply(persistentState.getCurrentTerm(), true, log.getLastIndex());
    }

    @Override
    public synchronized InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs args) {
        if (args.getTerm() < persistentState.getCurrentTerm()) {
            return new InstallSnapshotReply(persistentState.getCurrentTerm(), false);
        }
        if (args.getTerm() > persistentState.getCurrentTerm()) {
            becomeFollower(args.getTerm(), "saw higher term");
        }
        currentLeaderId = args.getLeaderId();
        stateMachine.installSnapshot(args.getData());
        log.applySnapshot(args.getLastIncludedIndex(), args.getLastIncludedTerm());
        volatileState.setCommitIndex(args.getLastIncludedIndex());
        volatileState.setLastApplied(args.getLastIncludedIndex());
        resetElectionTimeout();
        return new InstallSnapshotReply(persistentState.getCurrentTerm(), true);
    }

    // ------------------------------------------------------------------ replication (leader side)

    private void heartbeat() {
        for (String peer : members) {
            if (!peer.equals(config.getNodeId())) {
                replicateTo(peer);
            }
        }
    }

    private void replicateTo(String peer) {
        long next = nextIndex.getOrDefault(peer, log.getLastIndex() + 1);
        long snapshotIndex = log.getLastSnapshotIndex();
        if (next <= snapshotIndex) {
            sendSnapshot(peer);
            return;
        }
        long last = log.getLastIndex();
        if (next > last) {
            sendAppend(peer, next - 1, List.of());
            return;
        }
        List<LogEntry> entries = log.getEntriesFrom(next);
        int max = config.getMaxEntriesPerAppend();
        if (max > 0 && entries.size() > max) {
            entries = entries.subList(0, max);
        }
        sendAppend(peer, next - 1, entries);
    }

    private void sendAppend(String peer, long prevLogIndex, List<LogEntry> entries) {
        long prevLogTerm = log.getTermAt(prevLogIndex);
        AppendEntriesArgs args = new AppendEntriesArgs(
                persistentState.getCurrentTerm(), config.getNodeId(), prevLogIndex, prevLogTerm,
                entries, volatileState.getCommitIndex());
        publish(EventType.HEARTBEAT_SENT, entries.isEmpty()
                ? Map.of("peer", peer)
                : Map.of("peer", peer, "index", Long.toString(entries.get(entries.size() - 1).getIndex())));
        transport.sendAppendEntries(peer, args).thenAccept(reply ->
                handleAsync(() -> handleAppendReply(peer, reply, entries)));
    }

    private void sendSnapshot(String peer) {
        if (lastSnapshotData == null) {
            maybeSnapshot();
        }
        if (lastSnapshotData == null) {
            return;
        }
        long index = log.getLastSnapshotIndex();
        long term = log.getLastSnapshotTerm();
        InstallSnapshotArgs args = new InstallSnapshotArgs(
                persistentState.getCurrentTerm(), config.getNodeId(), index, term, lastSnapshotData);
        transport.sendInstallSnapshot(peer, args).thenAccept(reply ->
                handleAsync(() -> handleSnapshotReply(peer, reply)));
    }

    private void handleAppendReply(String peer, AppendEntriesReply reply, List<LogEntry> sent) {
        if (role != NodeRole.LEADER) {
            return;
        }
        if (reply.getTerm() > persistentState.getCurrentTerm()) {
            becomeFollower(reply.getTerm(), "higher term in append reply");
            return;
        }
        if (reply.isSuccess()) {
            long match = reply.getMatchIndex();
            matchIndex.put(peer, match);
            nextIndex.put(peer, match + 1);
            advanceCommitIndex();
        } else {
            long reduced = Math.max(1, nextIndex.getOrDefault(peer, 1L) - 1);
            nextIndex.put(peer, reduced);
            replicateTo(peer);
        }
    }

    private void handleSnapshotReply(String peer, InstallSnapshotReply reply) {
        if (role != NodeRole.LEADER) {
            return;
        }
        if (reply.getTerm() > persistentState.getCurrentTerm()) {
            becomeFollower(reply.getTerm(), "higher term in snapshot reply");
            return;
        }
        if (reply.isSuccess()) {
            long snap = log.getLastSnapshotIndex();
            matchIndex.put(peer, snap);
            nextIndex.put(peer, snap + 1);
            advanceCommitIndex();
        }
    }

    // ------------------------------------------------------------------ commit / apply

    private void advanceCommitIndex() {
        if (role != NodeRole.LEADER) {
            return;
        }
        long n = log.getLastIndex();
        for (long idx = volatileState.getCommitIndex() + 1; idx <= n; idx++) {
            if (log.getTermAt(idx) != persistentState.getCurrentTerm()) {
                continue; // only commit entries from the current term directly (§5.4.2)
            }
            int count = 1; // self
            for (String peer : members) {
                if (!peer.equals(config.getNodeId())
                        && matchIndex.getOrDefault(peer, 0L) >= idx) {
                    count++;
                }
            }
            if (count < majority) {
                break;
            }
            volatileState.setCommitIndex(idx);
            publish(EventType.ENTRY_COMMITTED, Map.of("index", Long.toString(idx)));
        }
        applyCommitted();
        maybeSnapshot();
    }

    private void applyCommitted() {
        while (volatileState.getLastApplied() < volatileState.getCommitIndex()) {
            long next = volatileState.getLastApplied() + 1;
            LogEntry entry = log.getEntryAt(next);
            if (entry == null) {
                break;
            }
            stateMachine.apply(entry.getCommand());
            volatileState.setLastApplied(next);
        }
    }

    private void maybeSnapshot() {
        if (!(stateMachine instanceof SnapshotProvider provider)) {
            return;
        }
        long threshold = config.getSnapshotThreshold();
        if (threshold <= 0) {
            return;
        }
        long last = log.getLastIndex();
        if (last > 0 && last - log.getLastSnapshotIndex() >= threshold) {
            byte[] data = provider.createSnapshot();
            long term = log.getTermAt(last);
            log.applySnapshot(last, term);
            lastSnapshotData = data;
            persistSnapshotToDisk(last, term, data);
        }
    }

    private void persistSnapshotToDisk(long lastIndex, long lastTerm, byte[] data) {
        try {
            Path dir = config.getDataDir();
            if (dir != null) {
                Files.createDirectories(dir);
                Files.write(dir.resolve("snapshot-" + lastIndex + "-" + lastTerm + ".bin"), data);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist snapshot", e);
        }
    }

    // ------------------------------------------------------------------ client-facing API

    /**
     * Submits a committed-when-replicated command to a leader. Returns the log
     * index it was assigned, or {@code -1} if this node is not the leader.
     */
    public synchronized long propose(byte[] command) {
        if (role != NodeRole.LEADER) {
            return -1;
        }
        long index = log.getLastIndex() + 1;
        log.append(new LogEntry(index, persistentState.getCurrentTerm(), command));
        publish(EventType.ENTRY_APPENDED, Map.of("index", Long.toString(index)));
        heartbeat();
        advanceCommitIndex();
        return index;
    }

    // ------------------------------------------------------------------ introspectable state (for tests/dashboard)

    public synchronized NodeRole getRole() { return role; }
    public synchronized String getNodeId() { return config.getNodeId(); }
    public synchronized String getLeaderId() { return currentLeaderId; }
    public synchronized long getCurrentTerm() { return persistentState.getCurrentTerm(); }
    public synchronized String getVotedFor() { return persistentState.getVotedFor(); }
    public synchronized long getCommitIndex() { return volatileState.getCommitIndex(); }
    public synchronized long getLastApplied() { return volatileState.getLastApplied(); }
    public synchronized long getLogLastIndex() { return log.getLastIndex(); }
    public synchronized Set<String> getMembers() { return Set.copyOf(members); }
    public boolean isLeader() { return role == NodeRole.LEADER; }

    // ------------------------------------------------------------------ helpers

    private void handleAsync(Runnable r) {
        if (!running) {
            return;
        }
        synchronized (RaftNode.this) {
            if (!running) {
                return;
            }
            r.run();
        }
    }

    private void publish(EventType type, Map<String, String> metadata) {
        if (telemetry == null) {
            return;
        }
        telemetry.publish(new RaftEvent(type, config.getNodeId(), persistentState.getCurrentTerm(),
                System.currentTimeMillis(), metadata));
    }

    public TransportClient getTransport() { return transport; }
    public StateMachine getStateMachine() { return stateMachine; }
}
