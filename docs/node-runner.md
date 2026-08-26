# node-runner — running and supervising the cluster

`node-runner` is the thin layer that wires `consensus-core`, `kv-store`,
`telemetry` and `chaos-harness` into something you can actually run and watch.

## Classes

| Class | Role |
|---|---|
| `NodeRunnerConfig` | immutable config for one node (id, peer addresses, ports, timing) |
| `NodeRunner` | boots one node; `start()`/`stop()` for kill/restart |
| `ClusterSupervisor` | owns N nodes + one shared `ChaosRules`; exposes a `ChaosController` |
| `ClusterSupervisorMain` | the runnable interactive CLI |

## `NodeRunnerConfig`

```java
NodeRunnerConfig(
    String nodeId,
    Map<String,String> peerAddresses,   // peerId -> "ws://127.0.0.1:<raftPort>"
    Path dataDir,                        // per-node storage
    int raftPort,                        // inbound consensus RPCs
    int telemetryPort,                   // dashboard event stream
    int heartbeatIntervalMs, int minElectionTimeoutMs, int maxElectionTimeoutMs,
    int maxEntriesPerAppend, long snapshotThreshold)
```

`peerAddresses` contains the **other** nodes only (a node never connects to
itself), and their keys become `RaftConfig.peerIds`.

## `NodeRunner` — one node, life-cycle safe

- Constructor creates the `TelemetryBus` + `MetricsCollector` once; they survive
  restarts so metric history is retained.
- `start()` builds `PersistentState`/`RaftLog`/`KVStateMachine`, the real
  transport, the chaos-wrapped transport, the `RaftNode`, the `RaftRpcServer`
  and the `TelemetryWebSocketServer`, then starts all of them and calls
  `node.start()`.
- `stop()` stops the node and both servers, closes state/log/transport, then
  nulls them — so `start()` can be called again and **reload persisted state
  from disk** (crashed-node recovery, `NFR-REL-3`).
- Accessors: `getNode()`, `isRunning()`, `getTelemetryBus()`,
  `getMetricsCollector()`, `getRaftPort()`, `getTelemetryPort()`, `getPeerIds()`.

## `ClusterSupervisor` — a whole cluster in one JVM

```java
ClusterSupervisor sup = new ClusterSupervisor();
sup.addNode(cfg);           // one per node
sup.startAll();
sup.currentLeaderId();      // Optional<String>
sup.controller();           // ChaosController
sup.rules();                // shared ChaosRules
sup.stopAll();
```

`controller()` returns an implementation whose `killNode`/`restartNode` drive the
`NodeRunner` lifecycle and whose `partition`/`heal`/`delay`/`drop`/`scenario`
mutate the **shared** `ChaosRules`. `currentLeaderId()` scans the running nodes
for the leader.

## `ClusterSupervisorMain` — the interactive CLI

Main class `node_runner.ClusterSupervisorMain`, program argument = node count
(default `3`). It assigns ports from fixed bases and starts every node:

```
raft base 7200 (step 20), telemetry base 8200 (step 20), admin base 9200 (step 20),
cluster admin on 9000
```

Commands:

| Command | Effect |
|---|---|
| `status` | every node's role, term, leader, commit/applied |
| `put <k> <v>` | propose a PUT via the leader, wait for commit |
| `get <k>` | read from the leader's applied state |
| `partition <A...> \| <B...>` | split into two groups (`\|` separates them) |
| `heal` | clear the partition |
| `kill <id>` | stop a node |
| `restart <id>` | start a killed node again |
| `delay <a> <b> <ms>` | one-way delay |
| `drop <a> <b> <p>` | one-way drop probability |
| `scenario <name>` | run a canned scenario (`split-brain`, `leader-churn`) |
| `scenarios` | list scenario names |
| `help` | command list |
| `quit` / `exit` | stop the cluster |

### Ports

| Node | raft | telemetry | admin |
|---|---|---|---|
| node-1 | 7200 | 8200 | 9200 |
| node-2 | 7220 | 8220 | 9220 |
| node-3 | 7240 | 8240 | 9240 |

Cluster admin (single endpoint, HTTP): **9000**. On start a `cluster.json`
descriptor is written to the temp data dir (see
[dashboard-contract.md](./dashboard-contract.md)). A dashboard connects to any
node's `telemetry` port; peers connect to the `raft` ports; the dashboard drives
the cluster through the `admin` (HTTP) endpoint. The `data=` shown is a temp
directory per node (`persistent.bin`, `raft.log`, optional `snapshot-*.bin`).

## Built-in scenarios

Both assume the default **3-node** cluster and run once on `scenario <name>`.
Times are ms from scenario start. See the full explanation in
[docs/README.md](./README.md) → "Setup of the built-in scenarios".

- **`split-brain`** — `partition node-1 | node-2,node-3` at `+1s`, `heal` at `+5s`.
- **`leader-churn`** — `kill node-1` at `+1s`, `restart node-1` at `+5s`,
  `setPartition {node-1:0, node-2:1, node-3:1}` at `+8s`, `heal` at `+12s`.

## Limitations / how to run it

- **One JVM, many logical nodes.** The supervisor runs all nodes as threads in a
  single process. This is deliberate: partition/delay/drop rely on a **shared**
  `ChaosRules`, which only works within one JVM. Spreading nodes across separate
  terminals would give each its own rules object and break cluster-wide fault
  consistency. The per-OS-process single-node `main()` and a shared chaos
  control plane are not yet built.
- Don't run two supervisors at once — the fixed ports will collide.
