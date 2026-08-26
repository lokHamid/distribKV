// chaos_harness/ChaosTransportClient.java
package chaos_harness;

import consensus_core.api.EventType;
import consensus_core.api.RaftEvent;
import consensus_core.api.TelemetryPublisher;
import consensus_core.rpc.AppendEntriesArgs;
import consensus_core.rpc.AppendEntriesReply;
import consensus_core.rpc.InstallSnapshotArgs;
import consensus_core.rpc.InstallSnapshotReply;
import consensus_core.rpc.RequestVoteArgs;
import consensus_core.rpc.RequestVoteReply;
import consensus_core.transport.TransportClient;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A transparent fault-injection proxy that sits on a node's outbound network
 * wire (FR-CHAOS-7). It wraps a real {@link TransportClient} and consults a
 * {@link ChaosRules} before forwarding: if the target is in a different
 * partition group the message is dropped (never delivered), if the link has a
 * configured drop rate the message may be dropped, and if the link is delayed
 * the message is held for that many milliseconds.
 *
 * <p>This is the ONLY place fault injection touches the message plane — the
 * Raft engine is entirely unaware of any of this.
 */
public class ChaosTransportClient implements TransportClient {

    private final String nodeId;
    private final TransportClient delegate;
    private final ChaosRules rules;
    private final TelemetryPublisher telemetry;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public ChaosTransportClient(String nodeId, TransportClient delegate, ChaosRules rules, TelemetryPublisher telemetry) {
        this.nodeId = nodeId;
        this.delegate = delegate;
        this.rules = rules;
        this.telemetry = telemetry;
    }

    @Override
    public CompletableFuture<RequestVoteReply> sendRequestVote(String targetNodeId, RequestVoteArgs args) {
        return dispatch(targetNodeId, () -> delegate.sendRequestVote(targetNodeId, args));
    }

    @Override
    public CompletableFuture<AppendEntriesReply> sendAppendEntries(String targetNodeId, AppendEntriesArgs args) {
        return dispatch(targetNodeId, () -> delegate.sendAppendEntries(targetNodeId, args));
    }

    @Override
    public CompletableFuture<InstallSnapshotReply> sendInstallSnapshot(String targetNodeId, InstallSnapshotArgs args) {
        return dispatch(targetNodeId, () -> delegate.sendInstallSnapshot(targetNodeId, args));
    }

    /**
     * Applies the current fault rules to a message addressed to {@code target}.
     * A dropped message returns a future that never completes, which models a
     * network that silently loses the packet (the caller times out naturally).
     */
    private <T> CompletableFuture<T> dispatch(String target, Supplier<CompletableFuture<T>> send) {
        String reason = rules.blockReason(nodeId, target);
        if (reason != null) {
            publish(EventType.CHAOS_MESSAGE_DROPPED, Map.of("peer", target, "reason", reason));
            return new CompletableFuture<>();
        }

        long delay = rules.delayMillis(nodeId, target);
        if (delay > 0) {
            publish(EventType.CHAOS_MESSAGE_DELAYED, Map.of("peer", target, "delayMillis", Long.toString(delay)));
            CompletableFuture<T> result = new CompletableFuture<>();
            scheduler.schedule(() -> forward(send, result), delay, TimeUnit.MILLISECONDS);
            return result;
        }
        return send.get();
    }

    private <T> void forward(Supplier<CompletableFuture<T>> send, CompletableFuture<T> result) {
        try {
            send.get().whenComplete((value, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(value);
                }
            });
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
    }

    private void publish(EventType type, Map<String, String> metadata) {
        if (telemetry != null) {
            telemetry.publish(new RaftEvent(type, nodeId, 0, System.currentTimeMillis(), metadata));
        }
    }

    /** Releases the delay scheduler. Idempotent. */
    public void close() {
        scheduler.shutdownNow();
    }
}
