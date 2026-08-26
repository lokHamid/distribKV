// node_runner/ClusterDescriptor.java
package node_runner;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Static description of the cluster so a dashboard (or a separate-JVM node
 * bootstrap) can discover every node's endpoints: its consensus (raft) port,
 * its telemetry (WebSocket) port, and its admin/control (HTTP) port.
 *
 * <p>Serialized to/from JSON via {@link Json}, and written to / read from a
 * {@code cluster.json} file (see {@link #writeTo(Path)} / {@link #readFrom(Path)}).
 */
public final class ClusterDescriptor {

    private final String name;
    private final int adminPort;
    private final List<NodeEndpoint> nodes;

    public ClusterDescriptor(String name, List<NodeEndpoint> nodes) {
        this(name, -1, nodes);
    }

    public ClusterDescriptor(String name, int adminPort, List<NodeEndpoint> nodes) {
        this.name = name;
        this.adminPort = adminPort;
        this.nodes = List.copyOf(nodes);
    }

    public String getName() { return name; }
    public int getAdminPort() { return adminPort; }
    public List<NodeEndpoint> getNodes() { return nodes; }
    public NodeEndpoint byId(String nodeId) {
        return nodes.stream().filter(n -> n.nodeId().equals(nodeId)).findFirst().orElse(null);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("adminPort", adminPort);
        List<Object> arr = new ArrayList<>();
        for (NodeEndpoint n : nodes) {
            arr.add(n.toMap());
        }
        m.put("nodes", arr);
        return m;
    }

    public String toJson() {
        return Json.write(toMap());
    }

    public void writeTo(Path path) throws java.io.IOException {
        Files.writeString(path, toJson(), StandardCharsets.UTF_8);
    }

    public static ClusterDescriptor fromJson(String json) {
        Map<String, Object> m = Json.parseObject(json);
        String name = (String) m.getOrDefault("name", "cluster");
        int adminPort = num(m.get("adminPort"), -1);
        List<NodeEndpoint> nodes = new ArrayList<>();
        Object raw = m.get("nodes");
        if (raw instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof Map<?, ?> mm) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mo = (Map<String, Object>) mm;
                    nodes.add(NodeEndpoint.fromMap(mo));
                }
            }
        }
        return new ClusterDescriptor(name, adminPort, nodes);
    }

    public static ClusterDescriptor readFrom(Path path) throws java.io.IOException {
        return fromJson(Files.readString(path, StandardCharsets.UTF_8));
    }

    private static int num(Object o, int def) {
        return o instanceof Number n ? n.intValue() : def;
    }

    /** One node's endpoints. */
    public record NodeEndpoint(String nodeId, String host, int raftPort, int telemetryPort, int adminPort) {

        public Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("nodeId", nodeId);
            m.put("host", host);
            m.put("raftPort", raftPort);
            m.put("telemetryPort", telemetryPort);
            m.put("adminPort", adminPort);
            return m;
        }

        @SuppressWarnings("unchecked")
        public static NodeEndpoint fromMap(Map<String, Object> m) {
            return new NodeEndpoint(
                    (String) m.get("nodeId"),
                    (String) m.getOrDefault("host", "127.0.0.1"),
                    num(m.get("raftPort"), -1),
                    num(m.get("telemetryPort"), -1),
                    num(m.get("adminPort"), -1));
        }

        private static int num(Object o, int def) {
            return o instanceof Number n ? n.intValue() : def;
        }
    }
}
