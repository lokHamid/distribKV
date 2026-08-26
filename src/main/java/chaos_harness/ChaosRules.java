// chaos_harness/ChaosRules.java
package chaos_harness;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Thread-safe, mutable holder of the injected network faults that a
 * {@link ChaosTransportClient} consults before forwarding a message.
 *
 * <p>When the cluster runs in a single supervisor JVM, every node's harness
 * shares one instance so a partition/delay/drop command is applied consistently
 * cluster-wide (FR-CHAOS-3/4/5, FR-CHAOS-7 — fault state lives entirely outside
 * consensus_core).
 */
public final class ChaosRules {

    /** Ordered pair of node ids. Faults are directional: (from -> to). */
    public record Link(String from, String to) {
    }

    private Map<String, Integer> partition = new HashMap<>();
    private final Map<Link, Long> delays = new HashMap<>();
    private final Map<Link, Double> dropRates = new HashMap<>();

    // ------------------------------------------------------------------ partition

    /** Replaces the partition assignment. Empty map = fully connected. */
    public synchronized void setPartition(Map<String, Integer> groups) {
        this.partition = new HashMap<>(groups);
    }

    public synchronized Map<String, Integer> getPartition() {
        return Map.copyOf(partition);
    }

    /** Removes the partition, restoring full connectivity (FR-CHAOS-4). */
    public synchronized void heal() {
        partition = new HashMap<>();
    }

    public synchronized boolean isPartitioned() {
        return !partition.isEmpty();
    }

    public synchronized boolean partitionsDifference(String from, String to) {
        if (partition.isEmpty()) {
            return false;
        }
        Integer gFrom = partition.get(from);
        Integer gTo = partition.get(to);
        return gFrom == null || gTo == null || !gFrom.equals(gTo);
    }

    // ------------------------------------------------------------------ delay

    public synchronized void delayLink(String from, String to, long delayMillis) {
        if (delayMillis < 0) {
            throw new IllegalArgumentException("delay must be >= 0");
        }
        delays.put(new Link(from, to), delayMillis);
    }

    public synchronized long delayMillis(String from, String to) {
        return delays.getOrDefault(new Link(from, to), 0L);
    }

    // ------------------------------------------------------------------ drop

    /** Sets the probability (0..1) that a message from {@code from} to {@code to} is dropped. */
    public synchronized void dropRate(String from, String to, double probability) {
        if (probability < 0 || probability > 1) {
            throw new IllegalArgumentException("drop probability must be in [0,1]");
        }
        dropRates.put(new Link(from, to), probability);
    }

    public synchronized double dropRateFor(String from, String to) {
        return dropRates.getOrDefault(new Link(from, to), 0.0);
    }

    // ------------------------------------------------------------------ decision

    /**
     * True if a message from {@code from} to {@code to} may be forwarded, i.e.
     * it is not blocked by a partition and not randomly dropped (FR-CHAOS-3/5).
     */
    public synchronized boolean shouldDeliver(String from, String to) {
        if (partitionsDifference(from, to)) {
            return false;
        }
        double rate = dropRateFor(from, to);
        return rate <= 0.0 || ThreadLocalRandom.current().nextDouble() >= rate;
    }

    /** Human-readable reason a message was blocked, or {@code null} if it was delivered. */
    public synchronized String blockReason(String from, String to) {
        if (partitionsDifference(from, to)) {
            return "partition";
        }
        double rate = dropRateFor(from, to);
        if (rate > 0.0 && ThreadLocalRandom.current().nextDouble() < rate) {
            return "drop";
        }
        return null;
    }

    /** Clears every fault currently configured. */
    public synchronized void clearAll() {
        partition = new HashMap<>();
        delays.clear();
        dropRates.clear();
    }

    public synchronized void clearLink(String from, String to) {
        delays.remove(new Link(from, to));
        dropRates.remove(new Link(from, to));
    }
}
