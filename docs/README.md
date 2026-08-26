# DistribKV — Project Documentation

DistribKV is a **Raft-based distributed key-value store** with a built-in
**fault-injection ("chaos") harness** and a **real-time telemetry** stream. This
folder documents the whole system: what each module does, how the pieces wire
together, and how the cluster re-converges *autonomously* after failures.

## Documentation index

| Document | What it covers |
|---|---|
| [README.md](./README.md) | This file — overview, build, run, quickstart |
| [architecture.md](./architecture.md) | Module map, wiring, and how the system works autonomously |
| [consensus-core.md](./consensus-core.md) | The Raft engine (election, replication, commit, persistence, snapshot) |
| [kv-store.md](./kv-store.md) | The replicated key-value state machine and its codecs |
| [telemetry.md](./telemetry.md) | Event bus, metrics, and the WebSocket event stream |
| [chaos-harness.md](./chaos-harness.md) | Fault-injection proxy layer (partition / delay / drop / kill / restart) |
| [node-runner.md](./node-runner.md) | Boots nodes; the cluster supervisor; the interactive CLI & scenarios |
| [dashboard-contract.md](./dashboard-contract.md) | The HTTP/WebSocket contract for the JavaFX dashboard |
| [testing.md](./testing.md) | The automated test suite and what it verifies |

## What it is

- **A single logical cluster** made of independent nodes. Each node runs the
  Raft consensus algorithm to elect a leader and replicate a log.
- **A key-value store** (`PUT`, `GET`, `DELETE`) whose state is produced by
  applying the *committed* Raft log — so every node converges to the same data.
- **A chaos layer** that sits *between* the Raft engine and the network. It
  injects real network faults (partitions, delays, drops) and node crashes
  (`kill`/`restart`) **without touching consensus code**.
- **A telemetry layer** that reports every meaningful transition (role, term,
  votes, commits, injected chaos) as JSON events over WebSocket, plus derived
  metrics (election duration, MTTR, commit latency, throughput).

## Tech stack

- **Java 21**, plain Maven project (`pom.xml`).
- Runtime deps: **Java-WebSocket 1.5.7**, **slf4j-api 2.0.13**.
- Test deps: **JUnit Jupiter 5.10.2** (via `maven-surefire-plugin`).

## Source layout

All code lives under `src/main/java` in five packages that map to the
"modules" from the SRS:

```
consensus_core/   Raft engine + transport + RPC
kv_store/         replicated KV state machine
telemetry/        event bus, metrics, WebSocket server
chaos_harness/    network fault-injection proxy + controller
node_runner/      boots nodes, supervises a cluster, interactive CLI
```

## Build

```bash
mvn compile        # compile
mvn test           # compile + run all tests
mvn package        # build the jar (runs tests)
```

> Maven is **not** guaranteed to be on your PATH. If `mvn` is unavailable, use
> the JUnit console launcher as in the test instructions (see
> [testing.md](./testing.md)).

## Run (dev/demo)

The quickest way to see the cluster, telemetry and chaos working is the
in-process supervisor:

```bash
mvn -q exec:java -Dexec.mainClass=node_runner.ClusterSupervisorMain -Dexec.args=3
```

or run `node_runner.ClusterSupervisorMain` from IntelliJ with program argument
`3`. From the IDE:

1. Right-click `pom.xml` → **Maven → Reload Project** (so Java-WebSocket/JUnit resolve).
2. Create a Run Configuration: main class `node_runner.ClusterSupervisorMain`, program arguments `3`, JDK 21.
3. Run. You get a 3-node cluster in one JVM plus an interactive console.

It prints each node's ports:

```
DistribKV — 3-node cluster (single JVM)
  node-1   raft=7200  telemetry=8200  data=temp-dir/node-1
  node-2   raft=7220  telemetry=8220  data=temp-dir/node-2
  node-3   raft=7240  telemetry=8240  data=temp-dir/node-3
```

- `raft=` is the port peers use for consensus RPCs.
- `telemetry=` is the WebSocket port a **dashboard** connects to for live events.

## Quickstart

```
distrokv> status            # see roles / terms / leader / commit
distrokv> put hello world   # write via the leader
distrokv> get hello         # read via the leader
distrokv> partition node-1 | node-2,node-3   # split the cluster
distrokv> status            # watch a new leader emerge on the majority side
distrokv> heal              # restore connectivity
distrokv> scenario split-brain   # run the canned chaos scripts
distrokv> quit
```

See [node-runner.md](./node-runner.md) for the full command reference and the
built-in chaos scenarios.
