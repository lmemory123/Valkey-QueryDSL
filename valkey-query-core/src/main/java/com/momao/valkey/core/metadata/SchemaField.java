package com.momao.valkey.core.metadata;

import com.momao.valkey.annotation.DistanceMetric;
import com.momao.valkey.annotation.FieldType;

public record SchemaField(
    String fieldName,
    String jsonPath,
    FieldType type,
    boolean sortable,
    double weight,
    boolean noStem,
    String separator,
    VectorOptions vectorOptions
) {

    public boolean effectiveSortable() {
        return type != FieldType.TAG && sortable;
    }

    public SchemaField {
        jsonPath = jsonPath == null || jsonPath.isEmpty() ? fieldName : jsonPath;
        separator = separator == null || separator.isEmpty() ? "," : separator;
    }

    public SchemaField(
            String fieldName,
            String jsonPath,
            FieldType type,
            boolean sortable,
            double weight,
            boolean noStem,
            String separator) {
        this(fieldName, jsonPath, type, sortable, weight, noStem, separator, null);
    }

    public SchemaField(String fieldName, FieldType type) {
        this(fieldName, fieldName, type, false, 1.0d, false, ",", null);
    }

    public static SchemaField text(String fieldName, double weight, boolean noStem, boolean sortable) {
        return text(fieldName, fieldName, weight, noStem, sortable);
    }

    public static SchemaField text(String fieldName, String jsonPath, double weight, boolean noStem, boolean sortable) {
        return new SchemaField(fieldName, jsonPath, FieldType.TEXT, sortable, weight, noStem, ",");
    }

    public static SchemaField tag(String fieldName, String separator, boolean sortable) {
        return tag(fieldName, fieldName, separator, sortable);
    }

    public static SchemaField tag(String fieldName, String jsonPath, String separator, boolean sortable) {
        return new SchemaField(fieldName, jsonPath, FieldType.TAG, false, 1.0d, false, separator);
    }

    public static SchemaField numeric(String fieldName, boolean sortable) {
        return numeric(fieldName, fieldName, sortable);
    }

    public static SchemaField numeric(String fieldName, String jsonPath, boolean sortable) {
        return new SchemaField(fieldName, jsonPath, FieldType.NUMERIC, sortable, 1.0d, false, ",");
    }

    public static SchemaField vector(String fieldName, int dimension, DistanceMetric distanceMetric, int m, int efConstruction) {
        return vector(fieldName, fieldName, dimension, distanceMetric, m, efConstruction);
    }

    public static SchemaField vector(
            String fieldName,
            String jsonPath,
            int dimension,
            DistanceMetric distanceMetric,
            int m,
            int efConstruction) {
        return new SchemaField(
                fieldName,
                jsonPath,
                FieldType.VECTOR,
                false,
                1.0d,
                false,
                ",",
                new VectorOptions(dimension, distanceMetric, m, efConstruction));
    }

    public record VectorOptions(
            int dimension,
            DistanceMetric distanceMetric,
            int m,
            int efConstruction
    ) {

        public VectorOptions {
            if (dimension <= 0) {
                throw new IllegalArgumentException("Vector dimension must be positive");
            }
            distanceMetric = distanceMetric == null ? DistanceMetric.COSINE : distanceMetric;
            if (m <= 0) {
                throw new IllegalArgumentException("Vector HNSW M must be positive");
            }
            if (efConstruction <= 0) {
                throw new IllegalArgumentException("Vector EF_CONSTRUCTION must be positive");
            }
        }
    }
}
