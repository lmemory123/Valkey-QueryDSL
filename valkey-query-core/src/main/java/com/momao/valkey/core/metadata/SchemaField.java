package com.momao.valkey.core.metadata;

import com.momao.valkey.annotation.FieldType;

public record SchemaField(
    String fieldName,
    String jsonPath,
    FieldType type,
    boolean sortable,
    double weight,
    boolean noStem,
    String separator
) {

    public SchemaField {
        jsonPath = jsonPath == null || jsonPath.isEmpty() ? fieldName : jsonPath;
        separator = separator == null || separator.isEmpty() ? "," : separator;
    }

    public SchemaField(String fieldName, FieldType type) {
        this(fieldName, fieldName, type, false, 1.0d, false, ",");
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
        return new SchemaField(fieldName, jsonPath, FieldType.TAG, sortable, 1.0d, false, separator);
    }

    public static SchemaField numeric(String fieldName, boolean sortable) {
        return numeric(fieldName, fieldName, sortable);
    }

    public static SchemaField numeric(String fieldName, String jsonPath, boolean sortable) {
        return new SchemaField(fieldName, jsonPath, FieldType.NUMERIC, sortable, 1.0d, false, ",");
    }
}
