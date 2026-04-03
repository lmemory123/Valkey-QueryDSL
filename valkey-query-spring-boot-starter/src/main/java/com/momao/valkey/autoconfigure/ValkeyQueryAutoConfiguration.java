package com.momao.valkey.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.ClusterValkeyClientRouting;
import com.momao.valkey.adapter.DefaultValkeyClientRouting;
import com.momao.valkey.adapter.ReadWriteSplitValkeyClientRouting;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.adapter.observability.SlowLogRecorder;
import com.momao.valkey.adapter.observability.ValkeyCommandMetricsRecorder;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import com.momao.valkey.core.exception.ValkeyConnectionException;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import glide.api.GlideClient;
import glide.api.models.configuration.AdvancedGlideClientConfiguration;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ProtocolVersion;
import glide.api.models.configuration.ReadFrom;
import glide.api.models.configuration.ServerCredentials;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

@AutoConfiguration
@EnableConfigurationProperties(ValkeyQueryProperties.class)
@ConditionalOnProperty(prefix = "valkey.query", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ValkeyQueryAutoConfiguration {

    private final ValkeyQueryProperties properties;

    public ValkeyQueryAutoConfiguration(ValkeyQueryProperties properties) {
        this.properties = properties;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public GlideClient glideClient() {
        return createClient(properties.resolveWriteNodes(), ReadFrom.PRIMARY);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public ValkeyClientRouting valkeyClientRouting(GlideClient glideClient) {
        if (properties.getMode() == TopologyMode.CLUSTER) {
            return new ClusterValkeyClientRouting(glideClient, this::createNodeClient);
        }
        if (properties.getMode() != TopologyMode.READ_WRITE_SPLIT) {
            return new DefaultValkeyClientRouting(glideClient);
        }
        GlideClient readClient = glideClient;
        boolean closeReadClient = false;
        if (properties.hasDedicatedReadNodes() && properties.getReadPreference() != com.momao.valkey.core.ReadPreference.PRIMARY) {
            readClient = createReadClient();
            closeReadClient = true;
        }
        return new ReadWriteSplitValkeyClientRouting(glideClient, readClient, properties.getReadPreference(), closeReadClient);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public SlowLogRecorder valkeySlowLogRecorder(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        ValkeyQueryProperties.Observability observability = properties.getObservability();
        if (!observability.isSlowLogEnabled()) {
            return SlowLogRecorder.NOOP;
        }
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        Counter droppedCounter = meterRegistry == null
                ? null
                : Counter.builder("valkey.query.slowlog.dropped")
                .description("Dropped Valkey slow-log records due to async recorder backpressure")
                .register(meterRegistry);
        SlowLogRecorder recorder = new LoggingSlowLogRecorder();
        if (!observability.isAsyncSlowLogEnabled()) {
            return recorder;
        }
        return new AsyncSlowLogRecorder(recorder, observability.getSlowLogThreadName(), droppedCounter);
    }

    @Bean
    @ConditionalOnMissingBean
    public ValkeyCommandMetricsRecorder valkeyCommandMetricsRecorder(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        ValkeyQueryProperties.Observability observability = properties.getObservability();
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        if (!observability.isEnabled() || meterRegistry == null) {
            return ValkeyCommandMetricsRecorder.NOOP;
        }
        return new MicrometerValkeyCommandMetricsRecorder(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public ValkeyUpdateMetricsRecorder valkeyUpdateMetricsRecorder(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        MeterRegistry meterRegistry = meterRegistryProvider.getIfAvailable();
        if (meterRegistry == null) {
            return ValkeyUpdateMetricsRecorder.NOOP;
        }
        return new MicrometerValkeyUpdateMetricsRecorder(meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    public ValkeyObservationInvoker valkeyObservationInvoker(
            ObjectProvider<ObservationRegistry> observationRegistryProvider,
            SlowLogRecorder slowLogRecorder,
            ValkeyCommandMetricsRecorder commandMetricsRecorder) {
        ValkeyQueryProperties.Observability observability = properties.getObservability();
        ObservationRegistry observationRegistry = observationRegistryProvider.getIfAvailable(() -> ObservationRegistry.NOOP);
        return new ValkeyObservationInvoker(
                observationRegistry,
                observability.isEnabled(),
                observability.isSlowLogEnabled(),
                observability.isTraceQueryTextEnabled(),
                observability.getSlowQueryThresholdMs() == null ? 0L : observability.getSlowQueryThresholdMs(),
                slowLogRecorder,
                commandMetricsRecorder
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public ValkeyConnectionInfo valkeyConnectionInfo() {
        return new ValkeyConnectionInfo(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper valkeyQueryObjectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }

    @Bean
    @ConditionalOnBean(ValkeyQueryPackages.class)
    @ConditionalOnMissingBean
    public ValkeyIndexManager valkeyIndexManager(
            ValkeyClientRouting clientRouting,
            ValkeyQueryPackages packages,
            ObjectProvider<MeterRegistry> meterRegistryProvider) {
        return new ValkeyIndexManager(clientRouting, packages, properties, ValkeyIndexMetrics.of(meterRegistryProvider.getIfAvailable()));
    }

    @Bean
    @ConditionalOnBean(ValkeyIndexManager.class)
    @ConditionalOnMissingBean
    public ValkeyIndexAutoCreator valkeyIndexAutoCreator(ValkeyIndexManager indexManager) {
        return new ValkeyIndexAutoCreator(indexManager);
    }

    public static class ValkeyConnectionInfo {

        private final ValkeyQueryProperties properties;

        public ValkeyConnectionInfo(ValkeyQueryProperties properties) {
            this.properties = properties;
        }

        public String describe() {
            if (properties.getMode() == TopologyMode.READ_WRITE_SPLIT) {
                return "mode=" + properties.getMode()
                        + ",readPreference=" + properties.getReadPreference()
                        + ",write=" + join(properties.resolveWriteNodes())
                        + ",read=" + join(properties.resolveReadNodes());
            }
            return "mode=" + properties.getMode()
                    + ",nodes=" + join(properties.resolveWriteNodes());
        }

        private String join(Iterable<ValkeyQueryProperties.Node> nodes) {
            StringBuilder builder = new StringBuilder();
            for (ValkeyQueryProperties.Node node : nodes) {
                if (!builder.isEmpty()) {
                    builder.append(",");
                }
                builder.append(node.getHost()).append(":").append(node.getPort());
            }
            return builder.isEmpty() ? "localhost:6379" : builder.toString();
        }
    }

    private GlideClient createClient(Iterable<ValkeyQueryProperties.Node> nodes) {
        return createClient(nodes, ReadFrom.PRIMARY);
    }

    private GlideClient createClient(Iterable<ValkeyQueryProperties.Node> nodes, ReadFrom readFrom) {
        try {
            return GlideClient.createClient(configureBuilder(nodes, readFrom).build()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyConnectionException(ValkeyErrorCode.CONNECTION_CLIENT_CREATE_FAILED, "创建 GlideClient 时线程被中断", exception);
        } catch (Exception exception) {
            throw new ValkeyConnectionException(ValkeyErrorCode.CONNECTION_CLIENT_CREATE_FAILED, "创建 GlideClient 失败", exception);
        }
    }

    private GlideClient createNodeClient(String host, int port) throws Exception {
        ValkeyQueryProperties.Node redirectedNode = properties.resolveClusterRedirectNode(host, port);
        ValkeyQueryProperties.Node announcedNode = new ValkeyQueryProperties.Node();
        announcedNode.setHost(host);
        announcedNode.setPort(port);
        if (redirectedNode.sameEndpoint(announcedNode)) {
            return createClient(java.util.List.of(redirectedNode));
        }
        try {
            return createClient(java.util.List.of(redirectedNode));
        } catch (ValkeyConnectionException exception) {
            return createClient(java.util.List.of(announcedNode));
        }
    }

    private GlideClient createReadClient() {
        java.util.LinkedHashMap<String, ValkeyQueryProperties.Node> mergedNodes = new java.util.LinkedHashMap<>();
        for (ValkeyQueryProperties.Node node : properties.resolveWriteNodes()) {
            mergedNodes.put(node.getHost() + ":" + node.getPort(), node);
        }
        for (ValkeyQueryProperties.Node node : properties.resolveReadNodes()) {
            mergedNodes.put(node.getHost() + ":" + node.getPort(), node);
        }
        return createClient(mergedNodes.values(), mapReadFrom(properties.getReadPreference()));
    }

    private GlideClientConfiguration.GlideClientConfigurationBuilder<?, ?> configureBuilder(
            Iterable<ValkeyQueryProperties.Node> nodes,
            ReadFrom readFrom) {
        GlideClientConfiguration.GlideClientConfigurationBuilder<?, ?> builder = GlideClientConfiguration.builder()
                .useTLS(properties.isUseTls())
                .readFrom(readFrom)
                .protocol(ProtocolVersion.RESP2)
                .requestTimeout(properties.getRequestTimeout())
                .advancedConfiguration(AdvancedGlideClientConfiguration.builder()
                        .connectionTimeout(properties.getConnectionTimeout())
                        .build());

        for (ValkeyQueryProperties.Node node : nodes) {
            builder.address(NodeAddress.builder()
                    .host(node.getHost())
                    .port(node.getPort())
                    .build());
        }

        if (StringUtils.hasText(properties.getPassword())) {
            ServerCredentials.ServerCredentialsBuilder credentialsBuilder = ServerCredentials.builder()
                    .password(properties.getPassword());
            if (StringUtils.hasText(properties.getUsername())) {
                credentialsBuilder.username(properties.getUsername());
            }
            builder.credentials(credentialsBuilder.build());
        }
        return builder;
    }

    private ReadFrom mapReadFrom(com.momao.valkey.core.ReadPreference readPreference) {
        if (readPreference == com.momao.valkey.core.ReadPreference.REPLICA_PREFERRED
                || readPreference == com.momao.valkey.core.ReadPreference.PRIMARY_PREFERRED) {
            return ReadFrom.PREFER_REPLICA;
        }
        return ReadFrom.PRIMARY;
    }
}
