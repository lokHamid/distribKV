# chaos-harness — the fault-injection layer

The chaos harness simulates real network and process failures **without any
change to consensus code**. It does this by being a *transparent proxy on the
outbound wire* plus an *operator control surface* for node lifecycles.

This satisfies the SRS chaos requirements:

- `FR-CHAOS-1/2` — kill / restart a node process.
- `FR-CHAOS-3/4` — partition / heal.
- `FR-CHAOS-5` — per-link delay and drop probability.
- `FR-CHAOS-6` — scripted, timed scenarios.
- `FR-CHAOS-7` — all injection happens at a transport/proxy layer, never in the
  consensus algorithm.

## The core principle

`RaftNode` sends messages through a `TransportClient`. The chaos harness is a
*decorator* around the real one:

```
RaftNode ──► ChaosTransportClient  ──► (real) WebSocketTransportClient ──► network
             │  consult ChaosRules
             │   • partition?  → drop (never deliver)
             │   • drop rate?  → maybe drop
             │   • delay?      → hold, then forward
             └─ publish CHAOS_* events to telemetry
```

Fault *state* lives in **`ChaosRules`**, entirely outside `consensus_core`. The
Raft engine never knows any of this exists.

## `ChaosRules` — the fault state

Thread-safe and mutable. When a cluster runs in one supervisor JVM, every node
shares a **single instance**, so a command is applied consistently everywhere.

```java
class ChaosRules {
    // partition
    void setPartition(Map<String,Integer> groups);   // nodeId -> group
    void heal();                                      // clear the partition
    boolean isPartitioned();
    boolean partitionsDifference(String from, String to);

    // one-way link faults (directional: from -> to)
    void delayLink(String from, String to, long ms);
    long   delayMillis(String from, String to);
    void   dropRate(String from, String to, double p);     // 0..1
    double dropRateFor(String from, String to);

    // decision asked by the harness
    boolean shouldDeliver(String from, String to);
    String  blockReason(String from, String to);            // "partition" | "drop" | null
    void clearLink(String from, String to);
    void clearAll();
}
```

- **Partition:** a message is delivered only if `from` and `to` are in the same
  group. Empty map = fully connected.
- **Drop:** with probability `p`, the message is dropped silently (the sender's
  future never completes — exactly like a lost packet; the caller times out).
- **Delay:** the message is held for `ms` then forwarded.

## `ChaosTransportClient` — the proxy

Implements `TransportClient`. On each outbound RPC it:

1. asks `ChaosRules.blockReason(nodeId, target)`; if non-null, **drops** the
   message, publishes `CHAOS_MESSAGE_DROPPED`, and returns a future that never
   completes (models a silently lost packet).
2. else if there is a link delay, it schedules the real send after `n`
   milliseconds and publishes `CHAOS_MESSAGE_DELAYED`.
3. else forwards immediately.

## `ChaosController` — the operator control surface

This is the administrative API (SRS §5.3), deliberately separate from the KV
client API so app code can't accidentally inject faults (`NFR-SEC-2`).

```java
interface ChaosController {
    void killNode(String nodeId);                 // FR-CHAOS-1
    void restartNode(String nodeId);              // FR-CHAOS-2
    void partition(Collection<String> groupA, Collection<String> groupB); // FR-CHAOS-3
    void heal();                                  // FR-CHAOS-4
    void delayLink(String a, String b, long ms);  // FR-CHAOS-5
    void dropRate(String a, String b, double p);  // FR-CHAOS-5
    void setPartition(Map<String,Integer> grouping);
    void runScenario(ChaosScenario scenario);     // FR-CHAOS-6
}
```

`ClusterSupervisor` provides the implementation: kill/restart stop & re-start a
`NodeRunner` (reloading persisted state), while `partition`/`heal`/`delay`/`drop`
mutate the shared `ChaosRules`.

## Scripted scenarios (`ChaosScenario` + runner)

A scenario is a named list of `(offsetMs, action, args)` steps built with a
DSL, sorted by offset, and executed on a background scheduler:

```java
ChaosScenario s = ChaosScenario.builder("t")
    .killAt(1000, "node-1")
    .partitionAt(3000, List.of("node-1"), List.of("node-2","node-3"))
    .healAt(6000)
    .build();

ChaosScenarioRunner.run(controller, s);   // schedules each step
```

Actions: `KILL`, `RESTART`, `PARTITION`, `HEAL`, `DELAY_LINK`, `DROP_RATE`,
`SET_PARTITION` — mapping 1:1 onto `ChaosController`.

## Where it plugs into a node

`NodeRunner` builds the chain:
`realTransport = new WebSocketTransportClient(peerAddresses)` then
`chaosTransport = new ChaosTransportClient(nodeId, realTransport, sharedRules, telemetryBus)`
and hands `chaosTransport` to `RaftNode`. The harness also gets the
`telemetryBus` so injected faults are observable in the same event stream the
dashboard consumes.

## Why a shared `ChaosRules` matters

If each node had its *own* rules object, `partition node-1 | node-2,node-3`
would block node-1's outbound messages but node-2/3 might still send to node-1 —
asymmetric and wrong. By sharing one rules instance across the whole cluster
(which the in-process supervisor does), the partition is applied identically on
every node. This is why the current dev mode runs the cluster in a single JVM;
separate OS processes would need a control plane to share fault state (not yet
built).
