# consensus-core — the Raft engine

`consensus_core` is the "brain": leader election, log replication, commit,
persistence, and snapshotting. It is **pure Java**, has no framework deps, and
is written only against two seams — `TransportClient` (outbound) and
`RaftRpcHandler` (inbound) — so the chaos harness can wrap the transport
without touching any consensus logic.

## Classes

| Class | Kind | Role |
|---|---|---|
| `node.RaftNode` | concrete | The Raft state machine + RPC handlers + timers |
| `node.RaftConfig` | immutable | node id, peers, data dir, timing, limits |
| `node.NodeRole` | enum | `FOLLOWER`, `CANDIDATE`, `LEADER` |
| `log.LogEntry` | value | `index`, `term`, `command` (bytes) |
| `log.RaftLog` | concrete | in-memory log + durable append/truncate |
| `state.PersistentState` | concrete | durable `currentTerm` + `votedFor` |
| `state.VolatileState` | concrete | `commitIndex` + `lastApplied` (not durable) |
| `rpc.*` | value | `RequestVote*`, `AppendEntries*`, `InstallSnapshot*` |
| `transport.TransportClient` | interface | outbound sends (3 RPCs) |
| `transport.RaftRpcHandler` | interface | inbound handlers (implemented by `RaftNode`) |
| `transport.RaftRpcServer` | concrete | WebSocket server → dispatches to the handler |
| `transport.WebSocketTransportClient` | concrete | WebSocket client → speaks to peers |
| `transport.RaftWireCodec` | utility | binary framing + per-RPC payload codecs |
| `api.StateMachine` | interface | `apply(byte[])`, `installSnapshot(byte[])` |
| `api.SnapshotProvider` | interface | `createSnapshot()` (optional compression) |
| `api.RaftEvent`, `api.EventType`, `api.TelemetryPublisher` | contract | telemetry emission |

## Election (FR-ELEC-*)

- Every node starts a `FOLLOWER` and schedules a **randomized** election timer
  in `[minElectionTimeoutMs, maxElectionTimeoutMs]`.
- On timeout → `becomeCandidate()`: increment term, vote for itself, request
  votes from all peers. It sends its last log term/index so voters can enforce
  the *up-to-date log* rule.
- `handleRequestVote`: grants only if (a) the candidate's term is >= our term,
  (b) we haven't already voted in that term, and (c) the candidate's log is at
  least as up to date as ours. On a **term change we reset `votedFor` to null**
  (Raft §5.2) — this is required so a node that was a candidate can later grant
  a vote in a newer term; without it a cluster livelocks in split votes.
- A candidate that gathers a quorum of votes → `becomeLeader()`: initializes
  `nextIndex`/`matchIndex` and starts heartbeating immediately.
- Seeing a higher term (in any RPC) → step down to `FOLLOWER` and adopt it.

## Log replication & commit (FR-LOG-*)

- The leader appends client commands via `propose(command)` and sends
  `AppendEntries` to follow `nextIndex`, honoring `maxEntriesPerAppend`.
- A follower checks the `prevLogIndex`/`prevLogTerm` consistency rule; on
  mismatch it rejects, and the leader backs off `nextIndex` and retries.
- On success the leader advances `matchIndex` and calls `advanceCommitIndex()`:
  it commits the highest index replicated to a quorum **for the current term**
  (Raft §5.4.2), publishes `ENTRY_COMMITTED`, and applies to the state machine.
- The leader ships `leaderCommit`; a follower advances its own `commitIndex`
  (capped at its last log index) and applies.

## Persistence

- `PersistentState` persists `currentTerm` + `votedFor` to `persistent.bin` on
  every mutation (flush + force). Rules: never regress term, at most one vote
  per term, never vote in an older term.
- `RaftLog` persists entries in a header + frames format:
  `[snapshotIndex:8][snapshotTerm:8]` then repeated `[index:8][term:8][len:4][cmd]`.
  It supports `append`, `getEntryAt`, `getTermAt`, `getEntriesFrom`,
  `truncateSuffix`, `applySnapshot`, and full reload via `load()`.

## Snapshots (FR-LOG-5)

- When the state machine implements `SnapshotProvider` and the log exceeds
  `snapshotThreshold`, `maybeSnapshot()` takes a snapshot, compacts the log,
  and writes `snapshot-<index>-<term>.bin`.
- `installSnapshot` is also handled on the wire so a lagging follower can
  catch up faster than replaying the whole log. (Threshold defaults to `0`,
  i.e. snapshotting is off until you raise it.)

## Transport & wire protocol

- `RaftRpcServer` is a WebSocket server bound to the node's raft port; it
  decodes frames and dispatches to the `RaftRpcHandler`.
- `WebSocketTransportClient` keeps one persistent WS connection per peer,
  multiplexes requests/replies by a monotonic correlation id, queues frames
  until the socket opens, and **evicts closed connections** so a dead peer never
  poisons a cached socket. Failed sends complete exceptionally so the election
  loop never dies.
- `RaftWireCodec` frame: `[type:1][correlationId:8][payloadLen:4][payload]` with
  message types `0..5` (RequestVote, RequestVoteReply, AppendEntries,
  AppendEntriesReply, InstallSnapshot, InstallSnapshotReply).

## Public RPC surface

```java
interface TransportClient {
    CompletableFuture<RequestVoteReply> sendRequestVote(String target, RequestVoteArgs args);
    CompletableFuture<AppendEntriesReply> sendAppendEntries(String target, AppendEntriesArgs args);
    CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String target, InstallSnapshotArgs args);
}

interface RaftRpcHandler {
    RequestVoteReply handleRequestVote(RequestVoteArgs args);
    AppendEntriesReply handleAppendEntries(AppendEntriesArgs args);
    InstallSnapshotReply handleInstallSnapshot(InstallSnapshotArgs args);
}

class RaftNode implements RaftRpcHandler {
    long propose(byte[] command);        // returns log index, or -1 if not leader
    NodeRole getRole(); boolean isLeader();
    String getLeaderId(); long getCurrentTerm();
    long getCommitIndex(); long getLastApplied(); long getLogLastIndex();
    void start(); void stop();
}
```

## Config (`RaftConfig`)

`(nodeId, peerIds, dataDir, heartbeatIntervalMs, minElectionTimeoutMs,
maxElectionTimeoutMs, maxEntriesPerAppend, snapshotThreshold)`.

`peerIds` are the **other** nodes (the node adds itself). The constructor
enforces `minElectionTimeoutMs > heartbeatIntervalMs` (broadcast time must be
well below the election timeout — a Raft timing requirement).
