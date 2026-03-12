package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.autoconfigure.ValkeyQueryAutoConfiguration;
import glide.api.GlideClient;
import glide.api.models.commands.scan.ScanOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Repository
public class SkuRepository extends BaseValkeyRepository<Sku> {

    private final ValkeyQueryAutoConfiguration.ValkeyConnectionInfo connectionInfo;

    @Autowired
    public SkuRepository(
            GlideClient glideClient,
            ObjectMapper objectMapper,
            ValkeyQueryAutoConfiguration.ValkeyConnectionInfo connectionInfo) {
        super(SkuQuery.METADATA, glideClient, Sku.class, objectMapper);
        this.connectionInfo = connectionInfo;
    }

    public void save(Sku sku) {
        if (sku.getId() == null || sku.getId().isBlank()) {
            sku.setId(UUID.randomUUID().toString());
        }
        save(sku.getId(), sku);
    }

    public Sku findById(String id) {
        try {
            Object rawJson = client.customCommand(new String[]{"JSON.GET", buildKey(id)}).get();
            if (rawJson == null) {
                return null;
            }
            String json = String.valueOf(rawJson);
            if (json.isBlank() || "null".equalsIgnoreCase(json)) {
                return null;
            }
            return objectMapper.readValue(json, Sku.class);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("读取商品时线程被中断: " + id, exception);
        } catch (Exception exception) {
            throw new IllegalStateException("读取商品失败: " + id, exception);
        }
    }

    public Map<String, Sku> findAll() {
        Map<String, Sku> result = new LinkedHashMap<>();
        try {
            String cursor = "0";
            ScanOptions options = ScanOptions.builder()
                    .matchPattern(getPrefix() + "*")
                    .count(200L)
                    .build();
            do {
                Object[] scanResult = client.scan(cursor, options).get();
                cursor = String.valueOf(scanResult[0]);
                for (Object keyObject : (Object[]) scanResult[1]) {
                    String key = String.valueOf(keyObject);
                    Object rawJson = client.customCommand(new String[]{"JSON.GET", key}).get();
                    if (rawJson != null) {
                        String json = String.valueOf(rawJson);
                        if (!json.isBlank() && !"null".equalsIgnoreCase(json)) {
                            result.put(key, objectMapper.readValue(json, Sku.class));
                        }
                    }
                }
            } while (!"0".equals(cursor));
            return result;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("扫描商品时线程被中断", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("扫描商品失败", exception);
        }
    }

    public String getConnectionInfo() {
        return connectionInfo.describe();
    }
}
