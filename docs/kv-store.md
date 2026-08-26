# kv-store — the replicated state machine

`kv_store` turns the committed Raft log into actual data. It has no knowledge of
Raft — it only implements the `StateMachine` contract that `RaftNode` calls when
an entry is committed. The only bridge between the two worlds is
`KVStateMachine`.

## Classes

| Class | Role |
|---|---|
| `KVStore` | the pure in-memory `Map<String,String>` (`ConcurrentHashMap`) |
| `KVStateMachine` | implements `StateMachine` + `SnapshotProvider`; the sole bridge |
| `KVCommand` | `(type, key, value)` value object |
| `CommandType` | `PUT`, `GET`, `DELETE` |
| `KVCommandCodec` | encode/decode a command to/from `byte[]` (log entry payload) |
| `KVResult` | `ok(value)` / `error(message)` |
| `KVResultCodec` | encode/decode a result to/from `byte[]` (returned to the caller) |
| `KVSnapshotCodec` | encode/decode the whole map for snapshots |

## How a write/read actually flows

1. A client calls `RaftNode.propose(commandBytes)` on the leader, where
   `commandBytes` came from `KVCommandCodec.encode(new KVCommand(PUT, k, v))`.
2. Raft replicates the entry to a quorum and commits it.
3. On commit, every node calls `stateMachine.apply(commandBytes)`.
4. `KVStateMachine.apply` decodes the command, mutates the `KVStore`, and
   returns `KVResultCodec.encode(result)`.

The result bytes are what a routed client call would receive back.

## `KVStateMachine.apply`

```java
KVCommand cmd = KVCommandCodec.decode(command);
KVResult result = switch (cmd.getType()) {
    case PUT    -> { store.put(cmd.getKey(), cmd.getValue()); yield KVResult.ok(null); }
    case DELETE -> { store.delete(cmd.getKey());             yield KVResult.ok(null); }
    case GET    -> {
        String v = store.get(cmd.getKey());
        yield v != null ? KVResult.ok(v) : KVResult.error("key not found");
    }
};
return KVResultCodec.encode(result);
```

## Wire formats

- **Command** (what lives in a log entry): `[typeOrdinal:1][keyLen:4][key][hasValue:1][valueLen:4][value]`.
- **Result** (returned to caller): `[success:1][hasValue:1][valueLen:4][value][errorLen:4][error]`.
- **Snapshot** (full map): `[count:4]` then `[writeUTF(key)][writeUTF(value)]*`.

## Consistency notes

- `PUT`/`DELETE` are only "successful" once committed by a quorum and applied —
  that is driven by Raft, not by the KV layer.
- `GET` in this build reads the applied state machine. Strict linearizable reads
  would add a leader read-index / lease step; the relaxed mode reads the
  (already-committed) local state. See the SRS `GR-KV-3`.
- `apply` is idempotent (last-write-wins for the same key), so a re-applied
  entry after recovery is safe.

## Snapshots

`KVStateMachine` implements `SnapshotProvider`; `createSnapshot()` returns
`KVSnapshotCodec.encode(store.snapshotView())`, and `installSnapshot(data)`
calls `store.restoreFrom(KVSnapshotCodec.decode(data))`. Combined with
`RaftLog.applySnapshot`, this lets a leader compact its log and lets a lagging
follower replace its whole state quickly.
