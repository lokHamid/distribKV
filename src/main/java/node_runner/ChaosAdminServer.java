// node_runner/ChaosAdminServer.java
package node_runner;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A tiny HTTP control server (SRS §5.3 admin API, kept separate from the client
 * API per NFR-SEC-2) that exposes live status/metrics and lets an operator — or
 * the JavaFX dashboard — inject chaos remotely. Uses only the JDK's
 * {@code com.sun.net.httpserver}.
 *
 * <p>Endpoints (all JSON):
 * <ul>
 *   <li>{@code GET /status} — array of {@link NodeStatus}</li>
 *   <li>{@code GET /metrics} — array of {@link NodeMetrics}</li>
 *   <li>{@code GET /cluster} — {@link ClusterDescriptor}</li>
 *   <li>{@code POST /control/kill?node=ID}</li>
 *   <li>{@code POST /control/restart?node=ID}</li>
 *   <li>{@code POST /control/heal}</li>
 *   <li>{@code POST /control/partition?a=A,B&b=C,D}</li>
 *   <li>{@code POST /control/partition-map?map=node-1:0,node-2:1}</li>
 *   <li>{@code POST /control/delay?a=A&b=B&ms=N}</li>
 *   <li>{@code POST /control/drop?a=A&b=B&p=0.5}</li>
 *   <li>{@code POST /control/scenario?name=NAME}</li>
 * </ul>
 */
public final class ChaosAdminServer {

    private final AdminBackend backend;
    private HttpServer server;
    private final int port;

    public ChaosAdminServer(int port, AdminBackend backend) {
        this.port = port;
        this.backend = backend;
    }

    /** Starts the server. Because port 0 is allowed, call {@link #getPort()} after starting. */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    public int getPort() {
        return server != null ? server.getAddress().getPort() : port;
    }

    // ------------------------------------------------------------------ handlers

    private void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            String method = ex.getRequestMethod();
            Map<String, String> query = parseQuery(ex.getRequestURI().getRawQuery());

            Object response;
            if (method.equals("GET") && path.equals("/status")) {
                response = statusesAsList();
            } else if (method.equals("GET") && path.equals("/metrics")) {
                response = metricsAsList();
            } else if (method.equals("GET") && path.equals("/cluster")) {
                response = backend.cluster() == null ? Map.of() : backend.cluster().toMap();
            } else if (method.equals("POST") && path.startsWith("/control/")) {
                response = control(path, query);
            } else if (method.equals("GET") && path.equals("/") ) {
                response = Map.of("service", "DistribKV admin", "hint",
                        "endpoints: /status /metrics /cluster /control/{kill,restart,heal,partition,delay,drop,scenario}");
            } else {
                respondJson(ex, 404, Map.of("ok", false, "error", "not found: " + method + " " + path));
                return;
            }
            respondJson(ex, 200, response);
        } catch (IllegalArgumentException e) {
            respondJson(ex, 400, Map.of("ok", false, "error", e.getMessage()));
        } catch (Exception e) {
            respondJson(ex, 500, Map.of("ok", false, "error", String.valueOf(e.getMessage())));
        }
    }

    private List<Object> statusesAsList() {
        List<Object> out = new ArrayList<>();
        for (NodeStatus s : backend.statuses()) {
            out.add(s.toMap());
        }
        return out;
    }

    private List<Object> metricsAsList() {
        List<Object> out = new ArrayList<>();
        for (NodeMetrics m : backend.metrics()) {
            out.add(m.toMap());
        }
        return out;
    }

    private Object control(String path, Map<String, String> q) {
        switch (path) {
            case "/control/kill" -> {
                require(q, "node");
                backend.controller().killNode(q.get("node"));
                return ok();
            }
            case "/control/restart" -> {
                require(q, "node");
                backend.controller().restartNode(q.get("node"));
                return ok();
            }
            case "/control/heal" -> {
                backend.controller().heal();
                return ok();
            }
            case "/control/partition" -> {
                require(q, "a");
                require(q, "b");
                backend.controller().partition(csv(q.get("a")), csv(q.get("b")));
                return ok();
            }
            case "/control/partition-map" -> {
                require(q, "map");
                backend.controller().setPartition(parseGrouping(q.get("map")));
                return ok();
            }
            case "/control/delay" -> {
                require(q, "a");
                require(q, "b");
                backend.controller().delayLink(q.get("a"), q.get("b"), Long.parseLong(require(q, "ms")));
                return ok();
            }
            case "/control/drop" -> {
                require(q, "a");
                require(q, "b");
                backend.controller().dropRate(q.get("a"), q.get("b"), Double.parseDouble(require(q, "p")));
                return ok();
            }
            case "/control/scenario" -> {
                String name = require(q, "name");
                backend.controller().runScenario(backend.scenarioByName(name));
                return ok();
            }
            default -> throw new IllegalArgumentException("unknown control endpoint: " + path);
        }
    }

    private static Map<String, Object> ok() {
        return Map.of("ok", true);
    }

    private static String require(Map<String, String> q, String key) {
        String v = q.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("missing query parameter '" + key + "'");
        }
        return v;
    }

    private static List<String> csv(String s) {
        List<String> out = new ArrayList<>();
        for (String part : s.split(",")) {
            if (!part.isBlank()) {
                out.add(part.trim());
            }
        }
        return out;
    }

    private static Map<String, Integer> parseGrouping(String s) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (String pair : s.split(",")) {
            String[] kv = pair.split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException("invalid group map entry: " + pair);
            }
            map.put(kv[0].trim(), Integer.parseInt(kv[1].trim()));
        }
        return map;
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> map = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return map;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                map.put(dec(pair), "");
            } else {
                map.put(dec(pair.substring(0, eq)), dec(pair.substring(eq + 1)));
            }
        }
        return map;
    }

    private static String dec(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static void respondJson(HttpExchange ex, int code, Object body) throws IOException {
        byte[] bytes = Json.write(body).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
