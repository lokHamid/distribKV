// telemetry/metrics/MetricsCollector.java
package telemetry.metrics;

import consensus_core.api.EventType;
import consensus_core.api.RaftEvent;
import telemetry.subscriber.TelemetrySubscriber;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Subscribes to the same bus as the dashboard, but instead of forwarding
 * events, it derives metrics from them: election duration, MTTR,
 * commit latency, throughput. Feeds FR-MET-2 / FR-MET-3 in the SRS.
 */
public class MetricsCollector implements TelemetrySubscriber {

    private static final long THROUGHPUT_WINDOW_MS = 1000;

    // --- MTTR tracking ---
    private final Map<String, Long> leaderLostAt = new ConcurrentHashMap<>();
    private volatile long lastMTTRMillis = -1;

    // --- Election count ---
    private final AtomicLong electionCount = new AtomicLong(0);

    // --- Commit latency tracking ---
    // index -> timestamp the entry was appended (by the leader), waiting to be matched
    // with its eventual ENTRY_COMMITTED event.
    private final Map<String, Long> appendedAt = new ConcurrentHashMap<>();
    private final AtomicLong totalLatencySumMillis = new AtomicLong(0);
    private final AtomicLong totalCommittedCount = new AtomicLong(0);

    // --- Throughput tracking (rolling window) ---
    private final ConcurrentLinkedDeque<Long> recentCommitTimestamps = new ConcurrentLinkedDeque<>();

    @Override
    public void onEvent(RaftEvent event) {
        switch (event.getType()) {
            case LEADER_LOST -> leaderLostAt.put(event.getNodeId(), event.getTimestamp());

            case LEADER_ELECTED -> {
                electionCount.incrementAndGet();
                Long lostAt = leaderLostAt.remove(event.getNodeId());
                if (lostAt != null) {
                    lastMTTRMillis = event.getTimestamp() - lostAt;
                }
            }

            case ENTRY_APPENDED -> {
                String index = event.getMetadata().get("index");
                if (index != null) {
                    appendedAt.put(index, event.getTimestamp());
                }
            }

            case ENTRY_COMMITTED -> {
                String index = event.getMetadata().get("index");
                Long appendTime = (index != null) ? appendedAt.remove(index) : null;
                if (appendTime != null) {
                    long latency = event.getTimestamp() - appendTime;
                    totalLatencySumMillis.addAndGet(latency);
                    totalCommittedCount.incrementAndGet();
                }
                recentCommitTimestamps.addLast(event.getTimestamp());
                pruneOldTimestamps(event.getTimestamp());
            }

            default -> { /* HEARTBEAT_SENT/RECEIVED, ROLE_CHANGED, etc. — not aggregated yet */ }
        }
    }

    private void pruneOldTimestamps(long now) {
        while (true) {
            Long oldest = recentCommitTimestamps.peekFirst();
            if (oldest == null || now - oldest <= THROUGHPUT_WINDOW_MS) break;
            recentCommitTimestamps.pollFirst();
        }
    }

    public long getLastMTTRMillis() {
        return lastMTTRMillis;
    }

    public MetricSnapshot snapshot() {
        long now = System.currentTimeMillis();
        pruneOldTimestamps(now); // ensure the window is current even if no recent commits triggered a prune

        long committedCount = totalCommittedCount.get();
        double avgLatency = committedCount == 0
                ? 0.0
                : (double) totalLatencySumMillis.get() / committedCount;

        long throughput = Math.round(
                recentCommitTimestamps.size() / (THROUGHPUT_WINDOW_MS / 1000.0)
        );

        return new MetricSnapshot(
                now,
                lastMTTRMillis,
                electionCount.get(),
                avgLatency,
                throughput
        );
    }
}