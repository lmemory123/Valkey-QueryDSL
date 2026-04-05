package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class AnnotationCoverageRepository extends BaseValkeyRepository<AnnotationCoverageDocument> {

    @Autowired
    public AnnotationCoverageRepository(
            ValkeyClientRouting clientRouting,
            ObjectMapper objectMapper,
            ValkeyObservationInvoker observationInvoker,
            ValkeyUpdateMetricsRecorder updateMetricsRecorder) {
        super(AnnotationCoverageDocumentSearchQuery.METADATA, clientRouting, AnnotationCoverageDocument.class, objectMapper, observationInvoker, updateMetricsRecorder);
    }
}
