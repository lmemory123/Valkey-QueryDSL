package com.momao.valkey.core;

import java.util.Arrays;

public record VectorQuery(
        String fieldName,
        float[] vector,
        int k,
        String scoreAlias
) {

    public VectorQuery {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("Vector fieldName cannot be blank");
        }
        if (vector == null || vector.length == 0) {
            throw new IllegalArgumentException("Vector query cannot be empty");
        }
        if (k <= 0) {
            throw new IllegalArgumentException("KNN k must be positive");
        }
        scoreAlias = scoreAlias == null || scoreAlias.isBlank() ? "__vector_score" : scoreAlias;
        vector = Arrays.copyOf(vector, vector.length);
    }
}
