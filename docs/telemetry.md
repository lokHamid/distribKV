# telemetry — the nervous system

Telemetry is how the system tells the outside world (and the futures dashboard)
what it is doing. It sits **outside** the consensus hot path: `RaftNode` and the
chaos harness *publish* events onto a bus, and subscribers consume them.

## Goals (SRS FR-MET-*, NFR-OBS-*)

- Log every consensus-relevant transition with a timestamp and node id.
- Measure election duration, MTTR, commit latency, and throughput.
- Expose a machine-readable JSON stream over WebSocket (independent of any GUI).

## The event pipeline

```
 RaftNode ──────┐
 ChaosTransportClient ──┐
                        ▼
                  TelemetryBus (TelemetryPublisher)
                        │  fan-out to subscribers
          ┌─────────────┼───────────────────┐
          ▼             ▼                   ▼
 MetricsCollector   WebSocketSubscriber  (any other subscriber)
          │             │
      MetricSnapshot   ws://…<telemetry port>  → dashboard
```

## Key classes

| Class | Role |
|---|---|
| `api.TelemetryPublisher` | `void publish(RaftEvent)` — what producers use |
| `api.RaftEvent` | immutable `(type, nodeId, term, timestamp, metadata)` |
| `api.EventType` | the vocabulary of events |
| `TelemetryBus` | implements `TelemetryPublisher`; thread-safe fan-out; `subscribe`/`unsubscribe` |
| `subscriber.TelemetrySubscriber` | `void onEvent(RaftEvent)` |
| `metrics.MetricsCollector` | a subscriber deriving metrics |
| `metrics.MetricSnapshot` | immutable metric snapshot + CSV row |
| `event.RaftEventCodec` | serializes a `RaftEvent` to JSON |
| `server.TelemetryWebSocketServer` | WebSocket server; wraps each client as a subscriber |
| `server.WebSocketSubscriber` | pushes JSON down one live socket |

## Events (`EventType`) and their metadata

| Event | When it fires | metadata |
|---|---|---|
| `NODE_STARTED` | node starts | – |
| `ROLE_CHANGED` | role transition | `from`, `to` |
| `TERM_CHANGED` | term advances | `term` |
| `VOTE_GRANTED` / `VOTE_DENIED` | vote decision | `votedFor` / `candidateId` |
| `LEADER_ELECTED` | a node wins election | `leaderId` |
| `LEADER_LOST` | a leader steps down / stops hearing | – |
| `ENTRY_APPENDED` | leader appends a command | `index` |
| `ENTRY_COMMITTED` | an entry reaches quorum | `index` |
| `HEARTBEAT_SENT` / `HEARTBEAT_RECEIVED` | keepalive | `peer` / `leaderId` |
| `CHAOS_PARTITIONED` | partition applied | `groups` |
| `CHAOS_HEALED` | partition healed | – |
| `CHAOS_NODE_KILLED` / `CHAOS_NODE_RESTARTED` | node lifecycle | `nodeId` |
| `CHAOS_MESSAGE_DROPPED` | harness drops a message | `peer`, `reason` (`partition`/`drop`) |
| `CHAOS_MESSAGE_DELAYED` | harness delays a message | `peer`, `delayMillis` |

## JSON event shape

`RaftEventCodec.toJson` produces:

```json
{"type":"LEADER_ELECTED","nodeId":"node-2","term":3,"timestamp":1699999999999,"metadata":{"leaderId":"node-2"}}
```

## The dashboard stream

`TelemetryWebSocketServer` extends the Java-WebSocket `WebSocketServer`. On
**open** it wraps the socket in a `WebSocketSubscriber` and subscribes it to the
bus; on **close** it unsubscribes. Every published event is pushed to that
socket as a text frame. Connect a client to `ws://127.0.0.1:<telemetry-port>` to
receive live JSON. `getConnectedDashboardCount()` reports the number of live
dashboards on that node.

## Metrics (`MetricsCollector`)

It subscribes to the same bus as the dashboards and derives numbers from events:

- **Election count & MTTR** — on `LEADER_LOST` it records `nodeId→timestamp`; on
  `LEADER_ELECTED` for that node it computes `elected − lost` and stores it as
  the last MTTR.
- **Commit latency** — on `ENTRY_APPENDED` it records `index→timestamp`; on
  `ENTRY_COMMITTED` for that index it accumulates `committed − appended` and the
  count, producing an average.
- **Throughput** — it keeps a rolling 1-second window of commit timestamps and
  reports commits/sec.

`MetricSnapshot` exposes
`(timestamp, lastMTTRMillis, electionCount, avgCommitLatencyMillis, throughputOpsPerSec)`
and `toCsvRow()` for export (FR-MET-3). A node keeps one `MetricsCollector`
across restarts, so history is retained during `kill`/`restart`.
