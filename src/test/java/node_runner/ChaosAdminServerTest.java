package node_runner;

import chaos_harness.ChaosController;
import chaos_harness.ChaosRules;
import chaos_harness.ChaosScenario;
import chaos_harness.ChaosScenarioRunner;
import telemetry.metrics.MetricSnapshot;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ChaosAdminServerTest {

    private static ChaosAdminServer server;

    private static final class StubController implements ChaosController {
        final AtomicInteger kills = new AtomicInteger();
        final AtomicInteger heals = new AtomicInteger();
        final ChaosRules rules = new ChaosRules();

        @Override public void killNode(String id) { kills.incrementAndGet(); }
        @Override public void restartNode(String id) { }
        @Override public void partition(Collection<String> a, Collection<String> b) { rules.setPartition(Map.of()); }
        @Override public void heal() { heals.incrementAndGet(); rules.heal(); }
        @Override public void delayLink(String a, String b, long ms) { rules.delayLink(a, b, ms); }
        @Override public void dropRate(String a, String b, double p) { rules.dropRate(a, b, p); }
        @Override public void setPartition(Map<String, Integer> g) { rules.setPartition(g); }
        @Override public void runScenario(ChaosScenario s) { ChaosScenarioRunner.run(this, s); }
    }

    private static final AdminBackend backend(StubController c) {
        return new AdminBackend() {
            @Override public List<NodeStatus> statuses() {
                return List.of(new NodeStatus("node-1", true, "LEADER", 2, "node-1", 3, 3, 3, "node-1"));
            }
            @Override public List<NodeMetrics> metrics() {
                return List.of(new NodeMetrics("node-1", new MetricSnapshot(1000, 150, 2, 20.0, 500)));
            }
            @Override public ClusterDescriptor cluster() {
                return new ClusterDescriptor("distrokv", 9000, List.of(
                        new ClusterDescriptor.NodeEndpoint("node-1", "127.0.0.1", 7200, 8200, 9200)));
            }
            @Override public ChaosController controller() { return c; }
            @Override public ChaosScenario scenarioByName(String name) { return ClusterScenarios.byName(name); }
        };
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static synchronized ChaosAdminServer server(StubController c) throws Exception {
        if (server == null) {
            server = new ChaosAdminServer(freePort(), backend(c));
            server.start();
        }
        return server;
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    private String get(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        try (BufferedReader r = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            return r.lines().reduce("", (x, y) -> x + y);
        }
    }

    private String post(String url) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().close();
        java.io.InputStream body = conn.getResponseCode() >= 400
                ? conn.getErrorStream() : conn.getInputStream();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            return r.lines().reduce("", (x, y) -> x + y);
        }
    }

    @Test
    void servesStatusMetricsAndCluster() throws Exception {
        ChaosAdminServer srv = server(new StubController());
        String root = "http://127.0.0.1:" + srv.getPort();

        String status = get(root + "/status");
        assertTrue(status.contains("\"role\":\"LEADER\""));
        assertTrue(status.contains("\"nodeId\":\"node-1\""));

        String metrics = get(root + "/metrics");
        assertTrue(metrics.contains("\"throughputOpsPerSec\":500"));

        String cluster = get(root + "/cluster");
        assertTrue(cluster.contains("\"adminPort\":9000"));
        assertTrue(cluster.contains("\"telemetryPort\":8200"));
    }

    @Test
    void appliesControlCommands() throws Exception {
        StubController c = new StubController();
        ChaosAdminServer srv = server(c);
        String root = "http://127.0.0.1:" + srv.getPort();

        String heal = post(root + "/control/heal");
        assertTrue(heal.contains("\"ok\":true"));
        assertEquals(1, c.heals.get());

        String kill = post(root + "/control/kill?node=" + URLEncoder.encode("node-2", StandardCharsets.UTF_8));
        assertTrue(kill.contains("\"ok\":true"));
        assertEquals(1, c.kills.get());

        String bad = post(root + "/control/partition");
        assertTrue(bad.contains("\"ok\":false")); // missing a/b
    }
}
