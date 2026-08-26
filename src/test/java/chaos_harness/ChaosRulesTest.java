package chaos_harness;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ChaosRulesTest {

    @Test
    void partitionBlocksCrossGroupDelivery() {
        ChaosRules rules = new ChaosRules();
        rules.setPartition(Map.of("a", 0, "b", 1, "c", 1));
        assertFalse(rules.shouldDeliver("a", "b"));
        assertTrue(rules.shouldDeliver("b", "c")); // same group -> deliver
        assertFalse(rules.shouldDeliver("a", "c"));
        assertEquals("partition", rules.blockReason("a", "b"));
        assertNull(rules.blockReason("b", "c"));
    }

    @Test
    void healRestoresConnectivity() {
        ChaosRules rules = new ChaosRules();
        rules.setPartition(Map.of("a", 0, "b", 1));
        assertFalse(rules.shouldDeliver("a", "b"));
        rules.heal();
        assertFalse(rules.isPartitioned());
        assertTrue(rules.shouldDeliver("a", "b"));
    }

    @Test
    void unpartitionedRuleYieldsFullDelivery() {
        ChaosRules rules = new ChaosRules();
        assertTrue(rules.shouldDeliver("a", "b"));
        assertNull(rules.blockReason("a", "b"));
    }

    @Test
    void deterministicDropRateDropsEveryMessage() {
        ChaosRules rules = new ChaosRules();
        rules.dropRate("a", "b", 1.0);
        for (int i = 0; i < 50; i++) {
            assertFalse(rules.shouldDeliver("a", "b"));
            assertEquals("drop", rules.blockReason("a", "b"));
        }
    }

    @Test
    void delayIsDirectional() {
        ChaosRules rules = new ChaosRules();
        rules.delayLink("a", "b", 500);
        assertEquals(500, rules.delayMillis("a", "b"));
        assertEquals(0, rules.delayMillis("b", "a"));
    }

    @Test
    void clearRestoresDeliverability() {
        ChaosRules rules = new ChaosRules();
        rules.dropRate("a", "b", 1.0);
        rules.clearLink("a", "b");
        assertTrue(rules.shouldDeliver("a", "b"));
    }
}
