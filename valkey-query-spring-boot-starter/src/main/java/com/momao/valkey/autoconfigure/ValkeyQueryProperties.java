package com.momao.valkey.autoconfigure;

import com.momao.valkey.core.ReadPreference;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "valkey.query")
public class ValkeyQueryProperties {

    private boolean enabled = true;

    private List<Node> nodes = defaultNodes();

    private TopologyMode mode = TopologyMode.STANDALONE;

    private ReadPreference readPreference = ReadPreference.PRIMARY;

    private String username;

    private String password;

    private Integer requestTimeout = 250;

    private Integer connectionTimeout = 2000;

    private boolean useTls = false;

    private EndpointGroup standalone = new EndpointGroup();

    private ReadWriteSplit readWrite = new ReadWriteSplit();

    private Cluster cluster = new Cluster();

    private IndexManagement indexManagement = new IndexManagement();

    private Observability observability = new Observability();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes == null || nodes.isEmpty() ? defaultNodes() : new ArrayList<>(nodes);
    }

    public TopologyMode getMode() {
        return mode;
    }

    public void setMode(TopologyMode mode) {
        this.mode = mode == null ? TopologyMode.STANDALONE : mode;
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public void setReadPreference(ReadPreference readPreference) {
        this.readPreference = readPreference == null ? ReadPreference.PRIMARY : readPreference;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Integer requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Integer connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public boolean isUseTls() {
        return useTls;
    }

    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    public EndpointGroup getStandalone() {
        return standalone;
    }

    public void setStandalone(EndpointGroup standalone) {
        this.standalone = standalone == null ? new EndpointGroup() : standalone;
    }

    public ReadWriteSplit getReadWrite() {
        return readWrite;
    }

    public void setReadWrite(ReadWriteSplit readWrite) {
        this.readWrite = readWrite == null ? new ReadWriteSplit() : readWrite;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster == null ? new Cluster() : cluster;
    }

    public Observability getObservability() {
        return observability;
    }

    public void setObservability(Observability observability) {
        this.observability = observability == null ? new Observability() : observability;
    }

    public IndexManagement getIndexManagement() {
        return indexManagement;
    }

    public void setIndexManagement(IndexManagement indexManagement) {
        this.indexManagement = indexManagement == null ? new IndexManagement() : indexManagement;
    }

    public List<Node> resolveWriteNodes() {
        if (mode == TopologyMode.READ_WRITE_SPLIT) {
            return normalizeNodes(readWrite.getWrite().getNodes(), nodes);
        }
        if (mode == TopologyMode.CLUSTER) {
            return normalizeNodes(cluster.getNodes(), nodes);
        }
        return normalizeNodes(standalone.getNodes(), nodes);
    }

    public List<Node> resolveReadNodes() {
        if (mode == TopologyMode.READ_WRITE_SPLIT) {
            return normalizeNodes(readWrite.getRead().getNodes(), resolveWriteNodes());
        }
        return resolveWriteNodes();
    }

    public Node resolveClusterRedirectNode(String announcedHost, int announcedPort) {
        Node announcedNode = createNode(announcedHost, announcedPort);
        if (mode != TopologyMode.CLUSTER) {
            return announcedNode;
        }
        List<Node> configuredNodes = resolveWriteNodes();
        for (Node configuredNode : configuredNodes) {
            if (configuredNode.getPort() == announcedPort) {
                return configuredNode.copy();
            }
        }
        if (configuredNodes.size() == 1) {
            Node seedNode = configuredNodes.get(0).copy();
            seedNode.setPort(announcedPort);
            return seedNode;
        }
        return announcedNode;
    }

    public boolean hasDedicatedReadNodes() {
        if (mode != TopologyMode.READ_WRITE_SPLIT) {
            return false;
        }
        List<Node> writeNodes = resolveWriteNodes();
        List<Node> readNodes = normalizeNodes(readWrite.getRead().getNodes(), writeNodes);
        return !sameNodes(writeNodes, readNodes);
    }

    public boolean preferPrimaryReads() {
        return readPreference == ReadPreference.PRIMARY || readPreference == ReadPreference.PRIMARY_PREFERRED;
    }

    private static List<Node> normalizeNodes(List<Node> configured, List<Node> fallback) {
        List<Node> source = configured == null || configured.isEmpty() ? fallback : configured;
        if (source == null || source.isEmpty()) {
            return defaultNodes();
        }
        return source.stream()
                .map(Node::copy)
                .toList();
    }

    private static boolean sameNodes(List<Node> left, List<Node> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int index = 0; index < left.size(); index++) {
            if (!left.get(index).sameEndpoint(right.get(index))) {
                return false;
            }
        }
        return true;
    }

    private static List<Node> defaultNodes() {
        List<Node> defaults = new ArrayList<>();
        defaults.add(new Node());
        return defaults;
    }

    private static Node createNode(String host, int port) {
        Node node = new Node();
        node.setHost(host);
        node.setPort(port);
        return node;
    }

    public static class EndpointGroup {

        private List<Node> nodes = new ArrayList<>();

        public EndpointGroup() {
        }

        public EndpointGroup(List<Node> nodes) {
            setNodes(nodes);
        }

        public List<Node> getNodes() {
            return nodes;
        }

        public void setNodes(List<Node> nodes) {
            this.nodes = nodes == null ? new ArrayList<>() : new ArrayList<>(nodes);
        }
    }

    public static class ReadWriteSplit {

        private EndpointGroup write = new EndpointGroup();

        private EndpointGroup read = new EndpointGroup();

        public EndpointGroup getWrite() {
            return write;
        }

        public void setWrite(EndpointGroup write) {
            this.write = write == null ? new EndpointGroup() : write;
        }

        public EndpointGroup getRead() {
            return read;
        }

        public void setRead(EndpointGroup read) {
            this.read = read == null ? new EndpointGroup() : read;
        }
    }

    public static class Cluster {

        private List<Node> nodes = new ArrayList<>();

        public List<Node> getNodes() {
            return nodes;
        }

        public void setNodes(List<Node> nodes) {
            this.nodes = nodes == null ? new ArrayList<>() : new ArrayList<>(nodes);
        }
    }

    public static class IndexManagement {

        private IndexManagementMode mode = IndexManagementMode.NONE;

        public IndexManagementMode getMode() {
            return mode;
        }

        public void setMode(IndexManagementMode mode) {
            this.mode = mode == null ? IndexManagementMode.NONE : mode;
        }
    }

    public static class Observability {

        private boolean enabled = true;

        private boolean slowLogEnabled = true;

        private boolean asyncSlowLogEnabled = true;

        private Integer slowQueryThresholdMs = 200;

        private boolean traceQueryTextEnabled = false;

        private String slowLogThreadName = "valkey-query-slowlog";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isSlowLogEnabled() {
            return slowLogEnabled;
        }

        public void setSlowLogEnabled(boolean slowLogEnabled) {
            this.slowLogEnabled = slowLogEnabled;
        }

        public boolean isAsyncSlowLogEnabled() {
            return asyncSlowLogEnabled;
        }

        public void setAsyncSlowLogEnabled(boolean asyncSlowLogEnabled) {
            this.asyncSlowLogEnabled = asyncSlowLogEnabled;
        }

        public Integer getSlowQueryThresholdMs() {
            return slowQueryThresholdMs;
        }

        public void setSlowQueryThresholdMs(Integer slowQueryThresholdMs) {
            this.slowQueryThresholdMs = slowQueryThresholdMs;
        }

        public boolean isTraceQueryTextEnabled() {
            return traceQueryTextEnabled;
        }

        public void setTraceQueryTextEnabled(boolean traceQueryTextEnabled) {
            this.traceQueryTextEnabled = traceQueryTextEnabled;
        }

        public String getSlowLogThreadName() {
            return slowLogThreadName;
        }

        public void setSlowLogThreadName(String slowLogThreadName) {
            this.slowLogThreadName = slowLogThreadName;
        }
    }

    public static class Node {

        private String host = "localhost";

        private Integer port = 6379;

        public String getHost() {
            return StringUtils.hasText(host) ? host : "localhost";
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port == null ? 6379 : port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public Node copy() {
            Node node = new Node();
            node.setHost(host);
            node.setPort(port);
            return node;
        }

        public boolean sameEndpoint(Node other) {
            return other != null
                    && normalizePort(port) == normalizePort(other.port)
                    && normalizeHost(host).equals(normalizeHost(other.host));
        }

        private static int normalizePort(Integer port) {
            return port == null ? 0 : port;
        }

        private static String normalizeHost(String host) {
            return StringUtils.hasText(host) ? host.trim() : "";
        }
    }
}
