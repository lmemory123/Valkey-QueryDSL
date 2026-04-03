package com.momao.valkey.adapter;

import com.momao.valkey.core.AggregateRow;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

record GroupKey(
        List<String> fields,
        List<Object> values
) {

    static GroupKey of(List<String> fields, AggregateRow row) {
        if (fields == null || fields.isEmpty()) {
            return new GroupKey(List.of(), List.of());
        }
        List<Object> values = new java.util.ArrayList<>(fields.size());
        for (String field : fields) {
            values.add(row.get(field));
        }
        return new GroupKey(List.copyOf(fields), List.copyOf(values));
    }

    Map<String, Object> toValueMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            map.put(fields.get(i), values.get(i));
        }
        return map;
    }
}
