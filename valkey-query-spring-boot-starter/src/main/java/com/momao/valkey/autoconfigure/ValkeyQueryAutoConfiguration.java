package com.momao.valkey.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import glide.api.GlideClient;
import glide.api.models.configuration.AdvancedGlideClientConfiguration;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ProtocolVersion;
import glide.api.models.configuration.ServerCredentials;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
        try {
            GlideClientConfiguration.GlideClientConfigurationBuilder<?, ?> builder = GlideClientConfiguration.builder()
                    .useTLS(properties.isUseTls())
                    .protocol(ProtocolVersion.RESP2)
                    .requestTimeout(properties.getRequestTimeout())
                    .advancedConfiguration(AdvancedGlideClientConfiguration.builder()
                            .connectionTimeout(properties.getConnectionTimeout())
                            .build());

            for (ValkeyQueryProperties.Node node : properties.getNodes()) {
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

            return GlideClient.createClient(builder.build()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("创建 GlideClient 时线程被中断", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("创建 GlideClient 失败", exception);
        }
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
    public ValkeyIndexAutoCreator valkeyIndexAutoCreator(GlideClient glideClient, ValkeyQueryPackages packages) {
        return new ValkeyIndexAutoCreator(glideClient, packages);
    }

    public static class ValkeyConnectionInfo {

        private final ValkeyQueryProperties properties;

        public ValkeyConnectionInfo(ValkeyQueryProperties properties) {
            this.properties = properties;
        }

        public String describe() {
            return properties.getNodes().stream()
                    .map(node -> node.getHost() + ":" + node.getPort())
                    .reduce((left, right) -> left + "," + right)
                    .orElse("localhost:6379");
        }
    }
}
