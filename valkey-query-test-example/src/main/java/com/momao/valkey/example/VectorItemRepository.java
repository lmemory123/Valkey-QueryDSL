package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;

final class VectorItemRepository extends BaseValkeyRepository<VectorItem> {

    VectorItemRepository(
            ValkeyClientRouting clientRouting,
            ObjectMapper objectMapper,
            ValkeyObservationInvoker observationInvoker,
            ValkeyUpdateMetricsRecorder updateMetricsRecorder) {
        super(VectorItemQuery.METADATA, clientRouting, VectorItem.class, objectMapper, observationInvoker, updateMetricsRecorder);
    }
}
