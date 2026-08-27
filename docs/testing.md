# testing — automated verification

The suite uses **JUnit 5** (`junit-platform-console-standalone` was used during
development; Maven runs it via `maven-surefire-plugin`). Tests live under
`src/test/java` mirroring the main packages.

## Run

```bash
mvn test
```

If Maven isn't installed you can run the console launcher directly (as done
during development):

```bash
# compile main + test into a build dir (classpath must include
# java-websocket, slf4j-api and the junit console standalone jar), then:
java -cp "<build>;<libs>" org.junit.platform.console.ConsoleLauncher --scan-classpath <build> --details=summary
```

## What is covered

| Test | Package | Verifies |
|---|---|---|
| `KVCommandCodecTest` | `kv_store.command` | PUT/GET/DELETE encode/decode round-trips |
| `KVResultCodecTest` | `kv_store.result` | success/value/error round-trips |
| `KVSnapshotCodecTest` | `kv_store.snapshot` | map → bytes → map |
| `KVStateMachineTest` | `kv_store` | applying PUT/GET/DELETE, installSnapshot replaces state |
| `RaftLogTest` | `consensus_core.log` | append, read, `getEntriesFrom`, truncate, persistence across reopen, snapshot compaction |
| `PersistentStateTest` | `consensus_core.state` | term/vote persistence, term never regresses, voting clears |
| `RaftWireCodecTest` | `consensus_core.transport` | all 6 RPC frame encode/decode round-trips |
| `WebSocketTransportTest` | `consensus_core.transport` | live RequestVote + AppendEntries round-trip over a real WebSocket server |
| `RaftNodeTest` | `consensus_core.node` | single node becomes leader & commits; vote grant/deny rules (older term, stale log, one vote per term) |
| `RaftEventCodecTest` | `telemetry.event` | JSON field shape + empty metadata |
| `MetricsCollectorTest` | `telemetry.metrics` | election count, MTTR, avg commit latency, throughput, stable snapshot |
| `ChaosRulesTest` | `chaos_harness` | partition blocks cross-group, heal restores, drop rate, directional delay |
| `ChaosTransportClientTest` | `chaos_harness` | pass-through, partition/drop never call the delegate, delayed delivery |
| `ChaosScenarioRunnerTest` | `chaos_harness` | steps execute in clock order |
| `ClusterSupervisorTest` | `node_runner` | **3-node cluster:** elect leader → commit a write → **kill the leader → a new leader is elected → commit again** → restart the dead node |
| `JsonTest` | `node_runner` | JSON encode/parse round-trips, escaping, malformed rejection |
| `ClusterDescriptorTest` | `node_runner` | descriptor JSON round-trip + `cluster.json` write/read |
| `NodeStatusTest` | `node_runner` | node status DTO (running + down) JSON round-trip |
| `ChaosAdminServerTest` | `node_runner` | admin HTTP endpoints over a live server: `/status`, `/metrics`, `/cluster`, `/control/*` |

There is also a small **dashboard test suite** in the sibling
`DistribKV-dashboard` project (`mvnw test`): `JsonTest` (JSON round-trips) and
`model.ModelParseTest` (contract DTO parsing). These are deliberately
non-UI tests so they run headless.

## Notable invariants the tests lock in

- `votedFor` resets when the term advances; a single node grants exactly one vote
  per term; a candidate with a stale log is denied.
- `RaftLog` and `PersistentState` survive reopen (durable restart).
- Fault injection never reaches the delegate when a partition/drop blocks a
  message; delays still deliver.
- A real 3-node cluster survives a leader crash without an operator and keeps
  committing (autonomous recovery).
