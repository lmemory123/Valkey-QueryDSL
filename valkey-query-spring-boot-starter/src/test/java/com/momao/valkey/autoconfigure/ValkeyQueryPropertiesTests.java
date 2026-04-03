package com.momao.valkey.autoconfigure;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValkeyQueryPropertiesTests {

    @Test
    void sameEndpointUsesValueEqualityForPort() {
        ValkeyQueryProperties.Node left = new ValkeyQueryProperties.Node();
        left.setHost("localhost");
        left.setPort(Integer.valueOf("6380"));

        ValkeyQueryProperties.Node right = new ValkeyQueryProperties.Node();
        right.setHost("localhost");
        right.setPort(Integer.valueOf("6380"));

        assertTrue(left.sameEndpoint(right));
    }

    @Test
    void dedicatedReadNodesTreatEquivalentEndpointsAsSame() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.setMode(TopologyMode.READ_WRITE_SPLIT);

        ValkeyQueryProperties.Node writeNode = new ValkeyQueryProperties.Node();
        writeNode.setHost("127.0.0.1");
        writeNode.setPort(Integer.valueOf("6380"));

        ValkeyQueryProperties.Node readNode = new ValkeyQueryProperties.Node();
        readNode.setHost("127.0.0.1");
        readNode.setPort(Integer.valueOf("6380"));

        properties.getReadWrite().getWrite().setNodes(List.of(writeNode));
        properties.getReadWrite().getRead().setNodes(List.of(readNode));

        assertFalse(properties.hasDedicatedReadNodes());
    }
}
