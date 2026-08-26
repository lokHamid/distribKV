// chaos_harness/ChaosScenario.java
package chaos_harness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A named, timed sequence of fault events for repeatable experiments
 * (FR-CHAOS-6). Each step has an offset (ms from scenario start) and an action
 * with arguments. Built through the {@link Builder} DSL.
 */
public final class ChaosScenario {

    /** Step actions that map 1:1 onto {@link ChaosController} operations. */
    public enum Action {
        KILL, RESTART, PARTITION, HEAL, DELAY_LINK, DROP_RATE, SET_PARTITION
    }

    public record Step(long offsetMillis, Action action, Map<String, Object> args) {
    }

    private final String name;
    private final List<Step> steps;

    private ChaosScenario(String name, List<Step> steps) {
        this.name = name;
        this.steps = List.copyOf(steps);
    }

    public String getName() { return name; }
    public List<Step> getSteps() { return steps; }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private final List<Step> steps = new ArrayList<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder killAt(long offset, String nodeId) {
            steps.add(new Step(offset, Action.KILL, Map.of("nodeId", nodeId)));
            return this;
        }

        public Builder restartAt(long offset, String nodeId) {
            steps.add(new Step(offset, Action.RESTART, Map.of("nodeId", nodeId)));
            return this;
        }

        public Builder partitionAt(long offset, List<String> groupA, List<String> groupB) {
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("groupA", List.copyOf(groupA));
            args.put("groupB", List.copyOf(groupB));
            steps.add(new Step(offset, Action.PARTITION, args));
            return this;
        }

        public Builder healAt(long offset) {
            steps.add(new Step(offset, Action.HEAL, Map.of()));
            return this;
        }

        public Builder delayAt(long offset, String a, String b, long millis) {
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("nodeA", a);
            args.put("nodeB", b);
            args.put("millis", millis);
            steps.add(new Step(offset, Action.DELAY_LINK, args));
            return this;
        }

        public Builder dropAt(long offset, String a, String b, double probability) {
            Map<String, Object> args = new LinkedHashMap<>();
            args.put("nodeA", a);
            args.put("nodeB", b);
            args.put("probability", probability);
            steps.add(new Step(offset, Action.DROP_RATE, args));
            return this;
        }

        public Builder setPartitionAt(long offset, Map<String, Integer> grouping) {
            steps.add(new Step(offset, Action.SET_PARTITION, Map.of("grouping", Map.copyOf(grouping))));
            return this;
        }

        public ChaosScenario build() {
            List<Step> sorted = new ArrayList<>(steps);
            sorted.sort((x, y) -> Long.compare(x.offsetMillis(), y.offsetMillis()));
            return new ChaosScenario(name, sorted);
        }
    }
}
