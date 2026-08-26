package node_runner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClusterDescriptorTest {

    @TempDir
    Path tempDir;

    @Test
    void roundTripsThroughJson() {
        ClusterDescriptor d = new ClusterDescriptor("distrokv", 9000, List.of(
                new ClusterDescriptor.NodeEndpoint("node-1", "127.0.0.1", 7200, 8200, 9200),
                new ClusterDescriptor.NodeEndpoint("node-3", "localhost", 7240, 8240, 9240)));

        ClusterDescriptor parsed = ClusterDescriptor.fromJson(d.toJson());
        assertEquals(d.getName(), parsed.getName());
        assertEquals(d.getAdminPort(), parsed.getAdminPort());
        assertEquals(2, parsed.getNodes().size());
        assertEquals("node-1", parsed.byId("node-1").nodeId());
        assertEquals(8240, parsed.byId("node-3").telemetryPort());
        assertEquals("localhost", parsed.byId("node-3").host());
    }

    @Test
    void writesAndReadsFile() throws Exception {
        ClusterDescriptor d = new ClusterDescriptor("c", 9000, List.of(
                new ClusterDescriptor.NodeEndpoint("node-1", "127.0.0.1", 7200, 8200, 9200)));
        Path file = tempDir.resolve("cluster.json");
        d.writeTo(file);

        ClusterDescriptor read = ClusterDescriptor.readFrom(file);
        assertEquals("c", read.getName());
        assertEquals(1, read.getNodes().size());
        assertEquals(7200, read.getNodes().get(0).raftPort());
    }
}
