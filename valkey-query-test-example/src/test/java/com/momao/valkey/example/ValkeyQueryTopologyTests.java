package com.momao.valkey.example;

import com.momao.valkey.autoconfigure.TopologyMode;
import com.momao.valkey.autoconfigure.ValkeyQueryAutoConfiguration;
import com.momao.valkey.autoconfigure.ValkeyQueryProperties;
import com.momao.valkey.core.ReadPreference;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValkeyQueryTopologyTests {

    @Test
    void standaloneModeFallsBackToLegacyNodes() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setNodes(List.of(node("10.0.0.10", 6379)));

        assertEquals("10.0.0.10", properties.resolveWriteNodes().get(0).getHost());
        assertEquals(6379, properties.resolveReadNodes().get(0).getPort());
        assertFalse(properties.hasDedicatedReadNodes());
    }

    @Test
    void readWriteSplitUsesDedicatedReadNodesAndReplicaPreference() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setMode(TopologyMode.READ_WRITE_SPLIT);
        properties.setReadPreference(ReadPreference.REPLICA_PREFERRED);
        properties.getReadWrite().getWrite().setNodes(List.of(node("10.0.0.11", 6379)));
        properties.getReadWrite().getRead().setNodes(List.of(node("10.0.0.12", 6380)));

        assertEquals("10.0.0.11", properties.resolveWriteNodes().get(0).getHost());
        assertEquals("10.0.0.12", properties.resolveReadNodes().get(0).getHost());
        assertTrue(properties.hasDedicatedReadNodes());
        assertFalse(properties.preferPrimaryReads());
    }

    @Test
    void readWriteSplitFallsBackToWriteNodesWhenReadNodesMissing() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setMode(TopologyMode.READ_WRITE_SPLIT);
        properties.getReadWrite().getWrite().setNodes(List.of(node("10.0.0.21", 6381)));

        assertEquals("10.0.0.21", properties.resolveWriteNodes().get(0).getHost());
        assertEquals("10.0.0.21", properties.resolveReadNodes().get(0).getHost());
        assertFalse(properties.hasDedicatedReadNodes());
    }

    @Test
    void connectionInfoIncludesTopologySummary() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setMode(TopologyMode.READ_WRITE_SPLIT);
        properties.setReadPreference(ReadPreference.REPLICA_PREFERRED);
        properties.getReadWrite().getWrite().setNodes(List.of(node("10.0.1.1", 6379)));
        properties.getReadWrite().getRead().setNodes(List.of(node("10.0.1.2", 6380)));

        String description = new ValkeyQueryAutoConfiguration.ValkeyConnectionInfo(properties).describe();

        assertTrue(description.contains("mode=READ_WRITE_SPLIT"));
        assertTrue(description.contains("readPreference=REPLICA_PREFERRED"));
        assertTrue(description.contains("write=10.0.1.1:6379"));
        assertTrue(description.contains("read=10.0.1.2:6380"));
    }

    @Test
    void clusterRedirectUsesConfiguredNodeWhenPortMatches() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setMode(TopologyMode.CLUSTER);
        properties.getCluster().setNodes(List.of(
                node("localhost", 8000),
                node("localhost", 8001),
                node("localhost", 8002)
        ));

        ValkeyQueryProperties.Node redirected = properties.resolveClusterRedirectNode("172.17.0.4", 8001);

        assertEquals("localhost", redirected.getHost());
        assertEquals(8001, redirected.getPort());
    }

    @Test
    void clusterRedirectFallsBackToSeedHostWhenOnlyOneNodeConfigured() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setMode(TopologyMode.CLUSTER);
        properties.getCluster().setNodes(List.of(node("localhost", 8000)));

        ValkeyQueryProperties.Node redirected = properties.resolveClusterRedirectNode("172.17.0.5", 8002);

        assertEquals("localhost", redirected.getHost());
        assertEquals(8002, redirected.getPort());
    }

    private static ValkeyQueryProperties.Node node(String host, int port) {
        ValkeyQueryProperties.Node node = new ValkeyQueryProperties.Node();
        node.setHost(host);
        node.setPort(port);
        return node;
    }
}
