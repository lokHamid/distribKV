# Dashboard ↔ Cluster contract

This is the *wire contract* the JavaFX dashboard consumes. It is plain JSON over
HTTP + WebSocket with **no shared library** — the dashboard implements its own
client against these shapes (which is why the backend uses a hand-rolled `Json`
instead of a JSON dependency).

There are three ways to reach a running cluster:

1. **Cluster admin (HTTP)** — one endpoint for the whole cluster (single-JVM
   supervisor). Use this for status, metrics, and injecting chaos.
2. **Per-node admin (HTTP)** — each node can expose its own in separate-JVM mode.
3. **Per-node telemetry (WebSocket)** — live event stream; one socket per node.

## Discovery: `cluster.json`

`ClusterSupervisorMain` writes `cluster.json` into its temp data dir and prints
the path. A dashboard loads it to discover endpoints.

```json
{
  "name": "distrokv",
  "adminPort": 9000,                       // cluster-wide admin (single-JVM)
  "nodes": [
    { "nodeId": "node-1", "host": "127.0.0.1", "raftPort": 7200, "telemetryPort": 8200, "adminPort": 9200 },
    { "nodeId": "node-2", "host": "127.0.0.1", "raftPort": 7220, "telemetryPort": 8220, "adminPort": 9220 },
    { "nodeId": "node-3", "host": "127.0.0.1", "raftPort": 7240, "telemetryPort": 8240, "adminPort": 9240 }
  ]
}
```

## Cluster admin HTTP endpoints

Base: `http://<host>:<clusterAdminPort>` (e.g. `http://127.0.0.1:9000`).

### `GET /status`
Array of node statuses:

```json
[
  { "nodeId":"node-1","running":true,"role":"LEADER","term":3,
    "leaderId":"node-1","commitIndex":5,"lastApplied":5,"logLastIndex":5,"votedFor":"node-1" },
  { "nodeId":"node-2","running":true,"role":"FOLLOWER","term":3,
    "leaderId":"node-1","commitIndex":5,"lastApplied":5,"logLastIndex":5,"votedFor":"node-1" },
  { "nodeId":"node-3","running":false,"role":"DOWN","term":-1,
    "leaderId":null,"commitIndex":-1,"lastApplied":-1,"logLastIndex":-1,"votedFor":null }
]
```

### `GET /metrics`
Array of per-node metrics:

```json
[
  { "nodeId":"node-1","timestamp":..., "lastMTTRMillis":150,
    "electionCount":2,"avgCommitLatencyMillis":20.0,"throughputOpsPerSec":500 }
]
```

### `GET /cluster`
The cluster descriptor object above.

### `POST /control/*`
All return `{ "ok": true }` on success or `{ "ok": false, "error": "..." }`
(HTTP 400) on failure.

| Endpoint | Query params | Effect |
|---|---|---|
| `/control/kill` | `node` | stop a node |
| `/control/restart` | `node` | start a stopped node |
| `/control/heal` | — | restore connectivity |
| `/control/partition` | `a=A,B` `b=C,D` | split into two groups |
| `/control/partition-map` | `map=node-1:0,node-2:1` | arbitrary grouping |
| `/control/delay` | `a` `b` `ms` | one-way delay |
| `/control/drop` | `a` `b` `p` | one-way drop probability (0..1) |
| `/control/scenario` | `name` | run built-in scenario |

Scenario names (via `/control/scenario?name=`): `split-brain`, `leader-churn`, `full`.

## Per-node telemetry (WebSocket)

Each node exposes `ws://<host>:<telemetryPort>`. On connect, the server registers
a subscriber and pushes every published `RaftEvent` as a JSON text frame:

```json
{"type":"LEADER_ELECTED","nodeId":"node-2","term":3,"timestamp":1699999999999,"metadata":{"leaderId":"node-2"}}
```

`type` is one of the `EventType` values (see `docs/telemetry.md`). A dashboard
typically opens one WS per node and fans events into a shared model. Events are
emitted for consensus transitions *and* chaos injections.

## Dashboard client

The JavaFX dashboard lives in the sibling project `../DistribKV-dashboard`
(module `org.lokman.distro_project_visual`, JavaFX 21 + JDK `java.net.http`; no
extra dependency):

| Class | Role |
|---|---|
| `DashboardApp` | the UI (node cards, chaos controls, metrics, event log) |
| `ClusterClient` | `fetchCluster`/`fetchStatuses`/`fetchMetrics`, all `control*` actions, and `openTelemetry` |
| `Json`, `model/*` | JSON parse + contract DTOs (`NodeSummary`, `NodeMetricsInfo`, `ClusterDescriptorInfo`, `RaftEventRecord`) |

It auto-connects to `http://127.0.0.1:9000` by default (change it in the header
and hit **Connect**). It opens one `openTelemetry` socket per node; each socket
**auto-reconnects with exponential backoff** and answers WebSocket PINGs, so the
event stream survives a node being killed/restarted or a brief network pause.

Run it:

```bash
cd ../DistribKV-dashboard
mvnw -q javafx:run      # JAVA_HOME must be set; a display is required
```

## Default ports (single-JVM supervisor, 3 nodes)

| Node | raft | telemetry | per-node admin |
|---|---|---|---|
| node-1 | 7200 | 8200 | 9200 |
| node-2 | 7220 | 8220 | 9220 |
| node-3 | 7240 | 8240 | 9240 |

Cluster admin (single endpoint): **9000**.
