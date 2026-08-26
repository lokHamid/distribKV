package chaos_harness;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ChaosScenarioRunnerTest {

    private static final class Recorder implements ChaosController {
        final AtomicLong kills = new AtomicLong();
        final AtomicLong heals = new AtomicLong();
        volatile boolean partitioned;
        volatile long lastDelay;

        @Override public void killNode(String id) { kills.incrementAndGet(); }
        @Override public void restartNode(String id) { }
        @Override public void partition(java.util.Collection<String> a, java.util.Collection<String> b) { partitioned = true; }
        @Override public void heal() { heals.incrementAndGet(); }
        @Override public void delayLink(String a, String b, long ms) { lastDelay = ms; }
        @Override public void dropRate(String a, String b, double p) { }
        @Override public void setPartition(Map<String, Integer> g) { }
        @Override public void runScenario(ChaosScenario s) { }
    }

    @Test
    void executesStepsInClockOrder() throws Exception {
        ChaosScenario scenario = ChaosScenario.builder("t")
                .killAt(50, "node-2")
                .healAt(200)
                .partitionAt(100, List.of("node-1"), List.of("node-2"))
                .delayAt(150, "node-1", "node-2", 700)
                .build();

        Recorder recorder = new Recorder();
        ChaosScenarioRunner.run(recorder, scenario, java.util.concurrent.Executors.newSingleThreadScheduledExecutor());

        Thread.sleep(400);
        assertEquals(1, recorder.kills.get());
        assertEquals(1, recorder.heals.get());
        assertTrue(recorder.partitioned);
        assertEquals(700, recorder.lastDelay);
    }
}
