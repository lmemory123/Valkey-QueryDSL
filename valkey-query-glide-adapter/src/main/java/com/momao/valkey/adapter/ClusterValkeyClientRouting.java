package com.momao.valkey.adapter;

import com.momao.valkey.core.BulkMode;
import com.momao.valkey.core.exception.ValkeyConnectionException;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import glide.api.GlideClient;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ClusterValkeyClientRouting implements ValkeyClientRouting {

    private static final Pattern MOVED_PATTERN = Pattern.compile("(?i)\\bMOVED\\b:?\\s*\\d+\\s+(\\S+)");
    private static final long DEFAULT_TOPOLOGY_CACHE_TTL_MILLIS = 1_000L;
    private static final int DEFAULT_MAX_REDIRECTED_CLIENTS = 32;

    private final GlideClient seedClient;

    private final NodeClientFactory nodeClientFactory;

    private final long topologyCacheTtlMillis;

    private final int maxRedirectedClients;

    private final LongSupplier clock;

    private final Object topologyMonitor = new Object();

    private final Map<String, GlideClient> topologyClients = new LinkedHashMap<>();

    private final Map<String, GlideClient> redirectedClients = new LinkedHashMap<>(16, 0.75f, true);

    private volatile ClusterTopology cachedTopology;

    public ClusterValkeyClientRouting(GlideClient seedClient, NodeClientFactory nodeClientFactory) {
        this(seedClient, nodeClientFactory, DEFAULT_TOPOLOGY_CACHE_TTL_MILLIS, DEFAULT_MAX_REDIRECTED_CLIENTS, System::currentTimeMillis);
    }

    ClusterValkeyClientRouting(
            GlideClient seedClient,
            NodeClientFactory nodeClientFactory,
            long topologyCacheTtlMillis,
            int maxRedirectedClients,
            LongSupplier clock) {
        this.seedClient = seedClient;
        this.nodeClientFactory = nodeClientFactory;
        this.topologyCacheTtlMillis = topologyCacheTtlMillis;
        this.maxRedirectedClients = maxRedirectedClients;
        this.clock = clock;
    }

    @Override
    public Object executeWrite(String[] command) throws Exception {
        return executeWithRedirect(command, seedClient);
    }

    @Override
    public Object executeRead(String[] command) throws Exception {
        return executeWithRedirect(command, seedClient);
    }

    @Override
    public Object executeIndex(String[] command) throws Exception {
        if (shouldBroadcastIndexCommand(command)) {
            List<Object> responses = executeIndexAll(command);
            return responses.isEmpty() ? null : responses.get(0);
        }
        return executeWrite(command);
    }

    @Override
    public List<Object> executeReadAll(String[] command) throws Exception {
        if (!isSearchCommand(command)) {
            return List.of(executeRead(command));
        }
        return executeAcrossCluster(command, false);
    }

    @Override
    public List<Object> executeIndexAll(String[] command) throws Exception {
        if (!shouldBroadcastIndexCommand(command)) {
            return List.of(Objects.requireNonNull(executeIndex(command)));
        }
        return executeAcrossCluster(command, true);
    }

    @Override
    public List<BatchCommandResult> executeWriteBatch(List<String[]> commands, BulkMode mode) {
        if (mode != BulkMode.UNORDERED || commands == null || commands.isEmpty()) {
            return ValkeyClientRouting.super.executeWriteBatch(commands, mode);
        }
        try {
            ClusterTopology topology = currentTopology();
            List<BatchCommandResult> results = new ArrayList<>(Collections.nCopies(commands.size(), null));
            Map<String, List<IndexedCommand>> groups = groupCommandsByNode(commands, topology);
            for (List<IndexedCommand> group : groups.values()) {
                if (group.isEmpty()) {
                    continue;
                }
                ClusterNode node = group.get(0).node();
                GlideClient client = clientFor(node);
                List<CompletableFuture<Object>> futures = new ArrayList<>(group.size());
                for (IndexedCommand indexedCommand : group) {
                    try {
                        futures.add(client.customCommand(indexedCommand.command()));
                    } catch (Exception exception) {
                        futures.add(null);
                        results.set(indexedCommand.index(), BatchCommandResult.failure(exception));
                    }
                }
                for (int index = 0; index < group.size(); index++) {
                    IndexedCommand indexedCommand = group.get(index);
                    if (results.get(indexedCommand.index()) != null) {
                        continue;
                    }
                    CompletableFuture<Object> future = futures.get(index);
                    try {
                        results.set(indexedCommand.index(), BatchCommandResult.success(future.get()));
                    } catch (Exception exception) {
                        results.set(indexedCommand.index(), BatchCommandResult.failure(exception));
                    }
                }
            }
            return results;
        } catch (Exception exception) {
            return ValkeyClientRouting.super.executeWriteBatch(commands, mode);
        }
    }

    @Override
    public boolean isClusterMode() {
        return true;
    }

    @Override
    public void close() throws Exception {
        List<GlideClient> clientsToClose = new ArrayList<>();
        synchronized (topologyClients) {
            clientsToClose.addAll(topologyClients.values());
            topologyClients.clear();
        }
        synchronized (redirectedClients) {
            clientsToClose.addAll(redirectedClients.values());
            redirectedClients.clear();
        }
        cachedTopology = null;
        List<AutoCloseable> closeables = new ArrayList<>(clientsToClose.size() + 1);
        closeables.add(seedClient);
        closeables.addAll(clientsToClose);
        CloseSupport.closeAll(closeables.toArray(AutoCloseable[]::new));
    }

    private Object executeWithRedirect(String[] command, GlideClient initialClient) throws Exception {
        GlideClient currentClient = initialClient;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                return currentClient.customCommand(command).get();
            } catch (Exception exception) {
                RedirectTarget redirectTarget = extractMovedTarget(exception);
                if (redirectTarget == null) {
                    throw exception;
                }
                currentClient = redirectClient(redirectTarget);
            }
        }
        throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_CLUSTER_REDIRECT_EXHAUSTED, "集群命令重定向次数过多: " + String.join(" ", command));
    }

    private List<Object> executeAcrossCluster(String[] command, boolean lenientMissingIndex) throws Exception {
        List<Object> responses = new ArrayList<>();
        for (GlideClient client : listMasterClients()) {
            try {
                responses.add(client.customCommand(command).get());
            } catch (Exception exception) {
                if (lenientMissingIndex && isMissingIndexError(exception)) {
                    continue;
                }
                throw exception;
            }
        }
        return responses;
    }

    private List<GlideClient> listMasterClients() throws Exception {
        return materializeMasterClients(currentTopology());
    }

    private ClusterTopology currentTopology() throws Exception {
        ClusterTopology topology = cachedTopology;
        long now = clock.getAsLong();
        if (topology != null && now < topology.expiresAtMillis()) {
            return topology;
        }
        synchronized (topologyMonitor) {
            topology = cachedTopology;
            now = clock.getAsLong();
            if (topology != null && now < topology.expiresAtMillis()) {
                return topology;
            }
            List<ClusterNode> nodes = loadMasterNodes();
            syncTopologyClients(nodes);
            ClusterTopology refreshed = new ClusterTopology(List.copyOf(nodes), now + topologyCacheTtlMillis);
            cachedTopology = refreshed;
            return refreshed;
        }
    }

    private List<ClusterNode> loadMasterNodes() throws Exception {
        Map<String, ClusterNode> nodes = new LinkedHashMap<>();
        Object response = seedClient.customCommand(new String[]{"CLUSTER", "NODES"}).get();
        String raw = String.valueOf(response);
        for (String line : raw.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split("\\s+");
            if (parts.length < 3) {
                continue;
            }
            String flags = parts[2];
            if (!flags.contains("master")) {
                continue;
            }
            RedirectTarget target = parseRedirectTarget(parts[1]);
            if (target == null) {
                continue;
            }
            boolean seedNode = flags.contains("myself");
            nodes.putIfAbsent(target.endpoint(), new ClusterNode(target, seedNode, parseSlotRanges(parts)));
        }
        return new ArrayList<>(nodes.values());
    }

    private List<GlideClient> materializeMasterClients(ClusterTopology topology) throws Exception {
        List<GlideClient> clients = new ArrayList<>(topology.nodes().size());
        for (ClusterNode node : topology.nodes()) {
            clients.add(clientFor(node));
        }
        return clients;
    }

    private GlideClient clientFor(ClusterNode node) throws Exception {
        return node.seedNode() ? seedClient : topologyClient(node.target());
    }

    private GlideClient topologyClient(RedirectTarget redirectTarget) throws Exception {
        GlideClient existing;
        synchronized (topologyClients) {
            existing = topologyClients.get(redirectTarget.endpoint());
            if (existing != null) {
                return existing;
            }
        }
        GlideClient created = createNodeClient(redirectTarget);
        synchronized (topologyClients) {
            existing = topologyClients.get(redirectTarget.endpoint());
            if (existing != null) {
                created.close();
                return existing;
            }
            topologyClients.put(redirectTarget.endpoint(), created);
            return created;
        }
    }

    private void syncTopologyClients(List<ClusterNode> nodes) throws Exception {
        Set<String> activeEndpoints = new LinkedHashSet<>();
        for (ClusterNode node : nodes) {
            if (!node.seedNode()) {
                activeEndpoints.add(node.target().endpoint());
            }
        }
        Map<String, GlideClient> evicted = new LinkedHashMap<>();
        synchronized (topologyClients) {
            topologyClients.keySet().removeIf(endpoint -> {
                boolean keep = activeEndpoints.contains(endpoint);
                if (!keep) {
                    evicted.put(endpoint, topologyClients.get(endpoint));
                }
                return !keep;
            });
        }
        for (GlideClient client : evicted.values()) {
            if (client != null) {
                client.close();
            }
        }
    }

    private GlideClient redirectClient(RedirectTarget redirectTarget) throws Exception {
        GlideClient existing;
        synchronized (redirectedClients) {
            existing = redirectedClients.get(redirectTarget.endpoint());
            if (existing != null) {
                return existing;
            }
        }
        GlideClient created = createNodeClient(redirectTarget);

        GlideClient evicted = null;
        synchronized (redirectedClients) {
            existing = redirectedClients.get(redirectTarget.endpoint());
            if (existing != null) {
                created.close();
                return existing;
            }
            redirectedClients.put(redirectTarget.endpoint(), created);
            if (redirectedClients.size() > maxRedirectedClients) {
                java.util.Iterator<Map.Entry<String, GlideClient>> iterator = redirectedClients.entrySet().iterator();
                if (iterator.hasNext()) {
                    Map.Entry<String, GlideClient> eldest = iterator.next();
                    evicted = eldest.getValue();
                    iterator.remove();
                }
            }
        }
        if (evicted != null) {
            evicted.close();
        }
        return created;
    }

    private GlideClient createNodeClient(RedirectTarget redirectTarget) throws Exception {
        try {
            return nodeClientFactory.create(redirectTarget.host(), redirectTarget.port());
        } catch (Exception creationException) {
            throw new ValkeyConnectionException(
                    ValkeyErrorCode.CONNECTION_CLUSTER_NODE_CREATE_FAILED,
                    "创建集群节点客户端失败: " + redirectTarget.endpoint(),
                    creationException
            );
        }
    }

    private RedirectTarget extractMovedTarget(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                Matcher matcher = MOVED_PATTERN.matcher(message);
                if (matcher.find()) {
                    RedirectTarget target = parseRedirectTarget(matcher.group(1));
                    if (target != null) {
                        return target;
                    }
                }
            }
            current = current.getCause();
        }
        return null;
    }

    private RedirectTarget parseRedirectTarget(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        String normalized = endpoint.trim();
        int busSeparator = normalized.indexOf('@');
        if (busSeparator >= 0) {
            normalized = normalized.substring(0, busSeparator);
        }
        if (normalized.startsWith("[")) {
            int endBracket = normalized.indexOf(']');
            if (endBracket <= 1 || endBracket + 1 >= normalized.length() || normalized.charAt(endBracket + 1) != ':') {
                return null;
            }
            String host = normalized.substring(1, endBracket);
            String portValue = normalized.substring(endBracket + 2);
            return toRedirectTarget(host, portValue);
        }
        int portSeparator = normalized.lastIndexOf(':');
        if (portSeparator <= 0 || portSeparator == normalized.length() - 1) {
            return null;
        }
        String host = normalized.substring(0, portSeparator);
        String portValue = normalized.substring(portSeparator + 1);
        return toRedirectTarget(host, portValue);
    }

    private RedirectTarget toRedirectTarget(String host, String portValue) {
        if (host == null || host.isBlank() || portValue == null || portValue.isBlank()) {
            return null;
        }
        try {
            return new RedirectTarget(host, Integer.parseInt(portValue));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private boolean isSearchCommand(String[] command) {
        if (command.length == 0) {
            return false;
        }
        String name = command[0];
        return "FT.SEARCH".equalsIgnoreCase(name) || "FT.AGGREGATE".equalsIgnoreCase(name);
    }

    private boolean shouldBroadcastIndexCommand(String[] command) {
        if (command.length == 0) {
            return false;
        }
        String name = command[0].toUpperCase();
        return "FT.CREATE".equals(name) || "FT.DROPINDEX".equals(name) || "FT._LIST".equals(name);
    }

    private boolean isMissingIndexError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("index with name")
                        || normalized.contains("unknown index name")
                        || normalized.contains("not found in database")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    public interface NodeClientFactory {

        GlideClient create(String host, int port) throws Exception;
    }

    private record RedirectTarget(String host, int port) {

        String endpoint() {
            return host + ":" + port;
        }
    }

    private Map<String, List<IndexedCommand>> groupCommandsByNode(List<String[]> commands, ClusterTopology topology) throws Exception {
        Map<String, List<IndexedCommand>> groups = new LinkedHashMap<>();
        for (int index = 0; index < commands.size(); index++) {
            String[] command = commands.get(index);
            String key = extractCommandKey(command);
            if (key == null) {
                throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_CLUSTER_REDIRECT_EXHAUSTED, "Cluster bulk write requires key-based commands");
            }
            ClusterNode node = topology.resolve(clusterSlot(key));
            if (node == null) {
                throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_CLUSTER_REDIRECT_EXHAUSTED, "Unable to resolve cluster slot owner for key: " + key);
            }
            groups.computeIfAbsent(node.target().endpoint(), ignored -> new ArrayList<>())
                    .add(new IndexedCommand(index, command, node));
        }
        return groups;
    }

    private String extractCommandKey(String[] command) {
        if (command == null || command.length < 2) {
            return null;
        }
        return command[1];
    }

    private int clusterSlot(String key) {
        String hashtag = extractHashTag(key);
        byte[] bytes = hashtag.getBytes(StandardCharsets.UTF_8);
        int crc = 0;
        for (byte value : bytes) {
            crc ^= (value & 0xFF) << 8;
            for (int bit = 0; bit < 8; bit++) {
                if ((crc & 0x8000) != 0) {
                    crc = (crc << 1) ^ 0x1021;
                } else {
                    crc <<= 1;
                }
                crc &= 0xFFFF;
            }
        }
        return crc % 16384;
    }

    private String extractHashTag(String key) {
        int start = key.indexOf('{');
        if (start < 0) {
            return key;
        }
        int end = key.indexOf('}', start + 1);
        if (end <= start + 1) {
            return key;
        }
        return key.substring(start + 1, end);
    }

    private List<SlotRange> parseSlotRanges(String[] parts) {
        List<SlotRange> ranges = new ArrayList<>();
        for (int index = 8; index < parts.length; index++) {
            String token = parts[index];
            if (token.isBlank() || token.startsWith("[")) {
                continue;
            }
            if (token.contains("-")) {
                String[] range = token.split("-", 2);
                try {
                    ranges.add(new SlotRange(Integer.parseInt(range[0]), Integer.parseInt(range[1])));
                } catch (NumberFormatException ignored) {
                    return List.of();
                }
                continue;
            }
            try {
                int slot = Integer.parseInt(token);
                ranges.add(new SlotRange(slot, slot));
            } catch (NumberFormatException ignored) {
                return List.of();
            }
        }
        return ranges;
    }

    private record ClusterTopology(List<ClusterNode> nodes, long expiresAtMillis) {

        ClusterNode resolve(int slot) {
            for (ClusterNode node : nodes) {
                if (node.owns(slot)) {
                    return node;
                }
            }
            return null;
        }
    }

    private record ClusterNode(RedirectTarget target, boolean seedNode, List<SlotRange> ranges) {

        boolean owns(int slot) {
            for (SlotRange range : ranges) {
                if (range.contains(slot)) {
                    return true;
                }
            }
            return false;
        }
    }

    private record SlotRange(int start, int end) {

        boolean contains(int slot) {
            return slot >= start && slot <= end;
        }
    }

    private record IndexedCommand(int index, String[] command, ClusterNode node) {
    }
}
