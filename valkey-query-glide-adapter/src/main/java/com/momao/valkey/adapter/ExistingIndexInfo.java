package com.momao.valkey.adapter;

import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.models.commands.FT.FTCreateOptions;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

final class ExistingIndexInfo {

    private final String dataType;
    private final List<String> prefixes;
    private final Map<String, ExistingFieldInfo> fields;

    private ExistingIndexInfo(String dataType, List<String> prefixes, Map<String, ExistingFieldInfo> fields) {
        this.dataType = dataType;
        this.prefixes = List.copyOf(prefixes);
        this.fields = Map.copyOf(fields);
    }

    static ExistingIndexInfo fromResponse(Object response) {
        Object[] infoItems = toArray(response);
        String dataType = null;
        List<String> prefixes = List.of();
        Map<String, ExistingFieldInfo> fields = new LinkedHashMap<>();

        for (int index = 0; index + 1 < infoItems.length; index += 2) {
            String key = asString(infoItems[index]).toLowerCase(Locale.ROOT);
            Object value = infoItems[index + 1];
            if ("index_definition".equals(key)) {
                IndexDefinition definition = parseDefinition(value);
                dataType = definition.dataType();
                prefixes = definition.prefixes();
                continue;
            }
            if ("attributes".equals(key)) {
                for (Object attribute : toArray(value)) {
                    ExistingFieldInfo field = parseField(attribute);
                    fields.put(field.alias(), field);
                }
            }
        }
        return new ExistingIndexInfo(dataType, prefixes, fields);
    }

    boolean matches(IndexSchema schema, FTCreateOptions.DataType expectedDataType) {
        if (!Objects.equals(normalizeDataType(dataType), expectedDataType.name())) {
            return false;
        }
        if (!new LinkedHashSet<>(prefixes).equals(new LinkedHashSet<>(schema.prefixes()))) {
            return false;
        }

        Map<String, ExistingFieldInfo> expectedFields = new LinkedHashMap<>();
        for (SchemaField field : schema.fields()) {
            expectedFields.put(field.fieldName(), ExistingFieldInfo.fromSchema(field, schema.storageType()));
        }
        return expectedFields.equals(fields);
    }

    private static IndexDefinition parseDefinition(Object value) {
        Object[] items = toArray(value);
        String dataType = null;
        List<String> prefixes = new ArrayList<>();
        for (int index = 0; index + 1 < items.length; index += 2) {
            String key = asString(items[index]).toLowerCase(Locale.ROOT);
            Object entryValue = items[index + 1];
            switch (key) {
                case "key_type", "data_type", "type", "index_type" -> dataType = normalizeDataType(asString(entryValue));
                case "prefixes" -> {
                    for (Object prefix : toArray(entryValue)) {
                        prefixes.add(asString(prefix));
                    }
                }
                default -> {
                }
            }
        }
        return new IndexDefinition(dataType, prefixes);
    }

    private static ExistingFieldInfo parseField(Object value) {
        Object[] items = toArray(value);
        String identifier = null;
        String alias = null;
        String type = null;
        String separator = ",";
        double weight = 1.0d;
        boolean noStem = false;
        boolean sortable = false;

        for (int index = 0; index < items.length; index++) {
            String token = asString(items[index]);
            String normalized = token.toLowerCase(Locale.ROOT);
            switch (normalized) {
                case "identifier" -> {
                    if (index + 1 < items.length) {
                        identifier = asString(items[++index]);
                    }
                }
                case "attribute" -> {
                    if (index + 1 < items.length) {
                        alias = asString(items[++index]);
                    }
                }
                case "type" -> {
                    if (index + 1 < items.length) {
                        type = normalizeFieldType(asString(items[++index]));
                    }
                }
                case "separator" -> {
                    if (index + 1 < items.length) {
                        separator = asString(items[++index]);
                    }
                }
                case "weight" -> {
                    if (index + 1 < items.length) {
                        weight = Double.parseDouble(asString(items[++index]));
                    }
                }
                case "nostem" -> noStem = true;
                case "sortable" -> sortable = true;
                default -> {
                }
            }
        }
        return new ExistingFieldInfo(alias, identifier, type, sortable, weight, noStem, separator);
    }

    private static String normalizeDataType(String raw) {
        if (raw == null) {
            return null;
        }
        return raw.toUpperCase(Locale.ROOT).replace("ON ", "").trim();
    }

    private static String normalizeFieldType(String raw) {
        return raw == null ? null : raw.toUpperCase(Locale.ROOT);
    }

    private static Object[] toArray(Object value) {
        if (value instanceof Object[] values) {
            return values;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            for (Object item : iterable) {
                values.add(item);
            }
            return values.toArray();
        }
        return value == null ? new Object[0] : new Object[]{value};
    }

    private static String asString(Object value) {
        return String.valueOf(value);
    }

    private record IndexDefinition(String dataType, List<String> prefixes) {
    }

    static final class ExistingFieldInfo {

        private final String alias;
        private final String identifier;
        private final String type;
        private final boolean sortable;
        private final double weight;
        private final boolean noStem;
        private final String separator;

        private ExistingFieldInfo(String alias, String identifier, String type, boolean sortable, double weight, boolean noStem, String separator) {
            this.alias = alias;
            this.identifier = identifier;
            this.type = type;
            this.sortable = sortable;
            this.weight = weight;
            this.noStem = noStem;
            this.separator = separator == null || separator.isEmpty() ? "," : separator;
        }

        static ExistingFieldInfo fromSchema(SchemaField field, StorageType storageType) {
            String identifier = storageType == StorageType.JSON ? "$." + field.jsonPath() : field.fieldName();
            return new ExistingFieldInfo(
                    field.fieldName(),
                    identifier,
                    field.type().name(),
                    field.sortable(),
                    field.weight(),
                    field.noStem(),
                    field.separator());
        }

        String alias() {
            return alias;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ExistingFieldInfo that)) {
                return false;
            }
            return sortable == that.sortable
                    && Double.compare(that.weight, weight) == 0
                    && noStem == that.noStem
                    && Objects.equals(alias, that.alias)
                    && Objects.equals(identifier, that.identifier)
                    && Objects.equals(type, that.type)
                    && Objects.equals(separator, that.separator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, identifier, type, sortable, weight, noStem, separator);
        }
    }
}
