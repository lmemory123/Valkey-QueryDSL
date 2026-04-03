package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import com.momao.valkey.autoconfigure.ValkeyQueryAutoConfiguration;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.exception.ValkeyQueryException;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.UUID;

@Repository
public class SkuRepository extends BaseValkeyRepository<Sku> {

    private final ValkeyQueryAutoConfiguration.ValkeyConnectionInfo connectionInfo;

    @Autowired
    public SkuRepository(
            ValkeyClientRouting clientRouting,
            ObjectMapper objectMapper,
            ValkeyObservationInvoker observationInvoker,
            ValkeyUpdateMetricsRecorder updateMetricsRecorder,
            ValkeyQueryAutoConfiguration.ValkeyConnectionInfo connectionInfo) {
        super(SkuQuery.METADATA, clientRouting, Sku.class, objectMapper, observationInvoker, updateMetricsRecorder);
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
            Object rawJson = executeReadCommand(new String[]{"JSON.GET", buildKey(id)});
            if (rawJson == null) {
                return null;
            }
            String json = String.valueOf(rawJson);
            if (json.isBlank() || "null".equalsIgnoreCase(json)) {
                return null;
            }
            return convertStoredFields(Map.of("$", json));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_ENTITY_READ_FAILED, "读取商品时线程被中断: " + id, exception);
        } catch (Exception exception) {
            if (exception instanceof ValkeyQueryException queryException) {
                throw queryException;
            }
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_ENTITY_READ_FAILED, "读取商品失败: " + id, exception);
        }
    }

    public String getConnectionInfo() {
        return connectionInfo.describe();
    }
}
