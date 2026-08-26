package telemetry.metrics;

import consensus_core.api.EventType;
import consensus_core.api.RaftEvent;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class MetricsCollectorTest {

    private RaftEvent event(EventType type, long ts, Map<String, String> metadata) {
        return new RaftEvent(type, "node-1", 1, ts, metadata);
    }

    @Test
    void measuresElectionCountAndMTTR() {
        MetricsCollector collector = new MetricsCollector();
        collector.onEvent(event(EventType.LEADER_LOST, 1000L, null));
        collector.onEvent(event(EventType.LEADER_ELECTED, 1150L, Map.of("leaderId", "node-1")));

        assertEquals(1, collector.snapshot().getElectionCount());
        assertEquals(150L, collector.getLastMTTRMillis());
    }

    @Test
    void measuresAvgCommitLatency() {
        MetricsCollector collector = new MetricsCollector();
        collector.onEvent(event(EventType.ENTRY_APPENDED, 2000L, Map.of("index", "5")));
        collector.onEvent(event(EventType.ENTRY_COMMITTED, 2030L, Map.of("index", "5")));
        collector.onEvent(event(EventType.ENTRY_APPENDED, 3000L, Map.of("index", "6")));
        collector.onEvent(event(EventType.ENTRY_COMMITTED, 3010L, Map.of("index", "6")));

        MetricSnapshot snapshot = collector.snapshot();
        assertEquals(20.0, snapshot.getAvgCommitLatencyMillis(), 0.0001);
    }

    @Test
    void throughputWithinWindowIsNonZeroAfterCommits() {
        MetricsCollector collector = new MetricsCollector();
        long base = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) {
            collector.onEvent(event(EventType.ENTRY_APPENDED, base + i, Map.of("index", Integer.toString(i))));
            collector.onEvent(event(EventType.ENTRY_COMMITTED, base + i + 5, Map.of("index", Integer.toString(i))));
        }
        assertTrue(collector.snapshot().getThroughputOpsPerSec() >= 1);
    }

    @Test
    void snapshotIsStable() {
        MetricsCollector collector = new MetricsCollector();
        MetricSnapshot s1 = collector.snapshot();
        MetricSnapshot s2 = collector.snapshot();
        assertEquals(s1.getElectionCount(), s2.getElectionCount());
        assertNotNull(s1.toCsvRow());
    }
}
