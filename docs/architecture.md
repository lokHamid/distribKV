# Architecture — how everything wires together

This describes the *physical* layout of one node, the *logical* layout of a
cluster, and how the pieces cooperate so the cluster works *autonomously*.

## One node (one JVM process)

Conceptually every node is a stack of four layers. `node-runner` is the tiny
`main()`-style class that assembles them into a runnable process.

```
            ┌────────────────────────────────────────────┐
            │                ONE NODE                    │
            │  consensus-core    (the brain — Raft)       │
            │  kv-store           (the memory — the data)  │
            │  telemetry          (nervous system — events)│
            │  chaos-harness     (filter on in/out wires)  │
            └───────────────┬────────────────────────────┘
                            │ network
                 talks to other identical processes
```

Layer by layer, and the concrete classes:

| Layer | Package | Key classes | Responsibility |
|---|---|---|---|
| consensus-core | `consensus_core` | `RaftNode`, `RaftLog`, `PersistentState`, `RaftRpcServer`, `WebSocketTransportClient` | Leader election, log replication, commit, persistence |
| kv-store | `kv_store` | `KVStore`, `KVStateMachine`, `KVCommandCodec`, `KVResultCodec`, `KVSnapshotCodec` | Apply committed commands to a shared map |
| telemetry | `telemetry` | `TelemetryBus`, `MetricsCollector`, `TelemetryWebSocketServer`, `RaftEventCodec` | Broadcast events, derive metrics, serve the dashboard |
| chaos-harness | `chaos_harness` | `ChaosRules`, `ChaosTransportClient`, `ChaosController`, `ChaosScenarioRunner` | Inject network faults & node crashes transparently |
| node-runner | `node_runner` | `NodeRunner`, `ClusterSupervisor`, `ClusterSupervisorMain` | Wire the above and boot/supervise the cluster |

## The two seams the layers plug into

Everything hangs off two **interfaces** in `consensus_core.transport`:

1. **`TransportClient`** — *outbound*. A node calls `sendRequestVote`,
   `sendAppendEntries`, `sendInstallSnapshot` to reach peers.
2. **`RaftRpcHandler`** — *inbound*. The node implements
   `handleRequestVote` / `handleAppendEntries` / `handleInstallSnapshot`;
   the local `RaftRpcServer` decodes incoming RPCs and dispatches here.

**This is the keystone of the whole design.** The Raft engine is written only
against these two interfaces. The *chaos-harness* is just another
`TransportClient` implementation — a wrapper that stands between `RaftNode`
and the real network transport. That is exactly how fault injection stays
entirely outside of consensus logic (FR-CHAOS-7).

## Wiring a single node

`NodeRunner.start()` assembles one node (simplified):

```
PersistentState  = new PersistentState(dataDir/persistent.bin)   // currentTerm + votedFor
RaftLog          = new RaftLog(dataDir/raft.log)                  // the replicated log
KVStateMachine   = new KVStateMachine(new KVStore())              // applies committed entries

realTransport    = new WebSocketTransportClient(peerAddresses)    // speaks to peer raft ports
chaosTransport   = new ChaosTransportClient(nodeId, realTransport, sharedRules, telemetryBus)
                   // ^ the chaos proxy: drops/delays/partitions on the way out

node             = new RaftNode(config, persistentState, volatileState,
                                 raftLog, stateMachine, chaosTransport, telemetryBus)

rpcServer        = new RaftRpcServer(node, raftPort)             // inbound consensus RPCs
telemetryServer  = new TelemetryWebSocketServer(telemetryBus, telemetryPort) // dashboard stream
```

On `start()`:
- `rpcServer.start()` and `telemetryServer.start()` open the sockets.
- `node.start()` loads persisted state + log and schedules a randomized
  election timeout.

## Wiring a whole cluster

`ClusterSupervisor` owns N `NodeRunner`s and — crucially — **shares one
`ChaosRules` instance** among all of them:

```
ClusterSupervisor
 ├─ chaosRules (shared) ───────────────────────────┐
 ├─ NodeRunner(node-1) ── ChaosTransportClient ────┤
 ├─ NodeRunner(node-2) ── ChaosTransportClient ────┤  all consult the same rules
 └─ NodeRunner(node-3) ── ChaosTransportClient ────┘
        │
        └─ controller()  →  modify rules / kill / restart
```

Because every harness reads the *same* rules object, a `partition` applies
identically on all nodes — otherwise node A could block node B while node B
still sends to node A. Storing the fault state once, in one place, is what
makes the chaos consistent cluster-wide.

## How it works autonomously

Nothing steers the cluster by hand. The system self-heals via Raft's
timeouts and quorums:

1. **Election.** Each node starts as a `FOLLOWER` with a randomized
   `[minElection,maxElection]` timer. If no valid heartbeat arrives before the
   timer fires, the node becomes a `CANDIDATE`, is leader in its own term, asks
   peers for votes (carrying its last-log term/index so voters only grant to an
   up-to-date candidate), and becomes `LEADER` on a quorum.
2. **Replication.** The leader appends client commands to its log and pushes
   `AppendEntries` to followers. It tracks `nextIndex`/`matchIndex` per peer and
   re-sends on failure; a follower rejects entries that violate the
   term/index log-matching rule.
3. **Commit.** An entry is committed when it is replicated to a quorum
   (`⌊n/2⌋+1`). The leader only commits entries from its current term
   directly (Raft §5.4.2); it propagates `leaderCommit`, and each node then
   **applies** committed entries to the `KVStateMachine`.
4. **Recovery.** If the leader dies, followers time out and elect someone new.
   If a node crashes and restarts, `node.start()` reloads `currentTerm`/`votedFor`
   and the log from disk and rejoins as a follower, catching up via replication
   or a snapshot. If a partition splits the cluster, the majority side elects a
   leader and keeps serving; the minority side stalls (it has no quorum) and
   reconciles after the partition heals.

All of the above happen *without an operator* stepping in; the operator only
*triggers* faults (`kill`, `partition`, …) and the consensus machinery does the
rest. That is what the chaos scenarios demonstrate.

## Cross-cutting: telemetry and chaos

- **Telemetry stays out of the hot path.** `RaftNode` and
  `ChaosTransportClient` *publish* immutable `RaftEvent`s onto the
  `TelemetryBus`; subscribers (metrics collector, per-dashboard socket) consume
  them asynchronously. Publishing never blocks consensus.
- **Chaos stays out of consensus.** `ChaosTransportClient` intercepts messages
  at the outbound seam and applies delay/drop/partition; the Raft engine never
  sees or knows about it. Killing a node is done by stopping the whole
  `NodeRunner` (process-level equivalent), which the consensus layer also never
  sees — it just notices the heartbeats stop.

## Ports and defaults

| Thing | Default | Notes |
|---|---|---|
| Raft RPC port (node `i`, 1-based) | `7200 + (i-1)*20` | peer-to-peer consensus |
| Telemetry port (node `i`) | `8200 + (i-1)*20` | dashboard connects here |
| Data dir | `temp-dir/<nodeId>` | per-node state/logs |
| Heartbeat interval | `60 ms` | |
| Election timeout | `200..400 ms` (random) | must be >> heartbeat |
| Max entries per AppendEntries | `100` | |
| Snapshot threshold | `0` (disabled) | enable by raising it |

## What is intentionally *not* here

- **out of scope / not yet built:** a per-OS-process single-node `main()`, a
  shared chaos control plane across separate JVMs, the JavaFX dashboard
  (`dashboard-fx`), the REST facade (`rest-facade`), and the client library.
  The current `ClusterSupervisorMain` runs the whole cluster in one JVM so that
  partition/delay/drop stay consistent. See [node-runner.md](./node-runner.md).
