package com.momao.valkey.core;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class ValkeyQueryChain<T> {

    private final ValkeyRepository<T> repository;
    private SearchCondition condition;
    private List<String> selectedFields = List.of();

    private String sortField;
    private boolean sortAscending = true;
    private VectorQuery vectorQuery;

    public ValkeyQueryChain(ValkeyRepository<T> repository) {
        this.repository = repository;
    }

    public ValkeyQueryChain<T> where(SearchCondition condition) {
        this.condition = condition;
        return this;
    }

    public ValkeyQueryChain<T> and(SearchCondition nextCondition) {
        if (this.condition == null) {
            this.condition = nextCondition;
        } else {
            this.condition = this.condition.and(nextCondition);
        }
        return this;
    }

    public ValkeyQueryChain<T> or(SearchCondition nextCondition) {
        if (this.condition == null) {
            this.condition = nextCondition;
        } else {
            this.condition = this.condition.or(nextCondition);
        }
        return this;
    }

    public ValkeyQueryChain<T> andIf(boolean execute, Supplier<SearchCondition> conditionSupplier) {
        if (execute) {
            this.and(conditionSupplier.get());
        }
        return this;
    }

    public ValkeyQueryChain<T> orIf(boolean execute, Supplier<SearchCondition> conditionSupplier) {
        if (execute) {
            this.or(conditionSupplier.get());
        }
        return this;
    }

    public ValkeyQueryChain<T> select(String... fields) {
        this.selectedFields = fields == null ? List.of() : java.util.Arrays.stream(fields)
                .filter(field -> field != null && !field.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
        return this;
    }

    public ValkeyQueryChain<T> orderByAsc(String field) {
        this.sortField = field;
        this.sortAscending = true;
        return this;
    }

    public ValkeyQueryChain<T> orderByDesc(String field) {
        this.sortField = field;
        this.sortAscending = false;
        return this;
    }

    public ValkeyQueryChain<T> knn(VectorFieldBuilder field, float[] vector, int k) {
        return knn(field, vector, k, "__vector_score");
    }

    public ValkeyQueryChain<T> knn(VectorFieldBuilder field, float[] vector, int k, String scoreAlias) {
        Objects.requireNonNull(field, "field");
        this.vectorQuery = new VectorQuery(field.fieldName(), vector, k, scoreAlias);
        return this;
    }

    public ValkeyQueryChain<T> knn(VectorFieldBuilder field, double[] vector, int k) {
        return knn(field, vector, k, "__vector_score");
    }

    public ValkeyQueryChain<T> knn(VectorFieldBuilder field, double[] vector, int k, String scoreAlias) {
        Objects.requireNonNull(vector, "vector");
        float[] values = new float[vector.length];
        for (int index = 0; index < vector.length; index++) {
            values[index] = (float) vector[index];
        }
        return knn(field, values, k, scoreAlias);
    }

    SearchCondition buildFinalCondition() {
        SearchCondition finalCond = this.condition != null ? this.condition : new SearchCondition("*");
        if (this.vectorQuery != null) {
            finalCond = finalCond.withVectorQuery(this.vectorQuery);
        }
        if (this.sortField != null) {
            if (this.vectorQuery != null && !this.sortField.equals(this.vectorQuery.scoreAlias())) {
                throw new IllegalStateException("Vector KNN queries only support ordering by the vector score alias: " + this.vectorQuery.scoreAlias());
            }
            finalCond = finalCond.sortBy(this.sortField, this.sortAscending);
        }
        if (!selectedFields.isEmpty()) {
            finalCond = finalCond.project(selectedFields);
        }
        return finalCond;
    }

    public List<T> list() {
        return repository.list(buildFinalCondition());
    }

    public <R> List<R> list(Function<T, R> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        List<T> values = repository.list(buildFinalCondition());
        List<R> mapped = new java.util.ArrayList<>(values.size());
        for (T value : values) {
            mapped.add(mapper.apply(value));
        }
        return mapped;
    }

    public Page<T> page(int offset, int limit) {
        return repository.page(buildFinalCondition(), offset, limit);
    }

    public <R> Page<R> page(int offset, int limit, Function<T, R> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        return repository.page(buildFinalCondition(), offset, limit).map(mapper);
    }

    public T one() {
        return repository.one(buildFinalCondition());
    }

    public <R> R one(Function<T, R> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        T value = repository.one(buildFinalCondition());
        return value == null ? null : mapper.apply(value);
    }

    public long count() {
        if (vectorQuery != null) {
            throw new UnsupportedOperationException("count() is not supported for vector KNN queries");
        }
        return repository.count(buildFinalCondition());
    }

    public long countDistinct(ValkeyFieldReference field) {
        if (vectorQuery != null) {
            throw new UnsupportedOperationException("countDistinct() is not supported for vector KNN queries");
        }
        List<String> fieldNames = requireDistinctFields(field);
        String fieldName = fieldNames.get(0);
        int offset = 0;
        int pageSize = 1_000;
        long total = 0L;
        while (true) {
            AggregateRequest request = new AggregateRequest(
                    fieldNames,
                    List.of(),
                    fieldName,
                    true,
                    offset,
                    pageSize
            );
            AggregateResult result = repository.aggregate(buildFinalCondition(), request);
            int size = result.rows().size();
            total += size;
            if (size < pageSize) {
                return total;
            }
            offset += pageSize;
        }
    }

    public List<String> distinct(ValkeyFieldReference field) {
        return distinct(field, 1_000);
    }

    public List<String> distinct(ValkeyFieldReference field, int limit) {
        AggregateResult result = distinctResult(0, limit, field);
        List<String> values = new java.util.ArrayList<>(result.rows().size());
        for (AggregateRow row : result.rows()) {
            Object value = row.get(field.fieldName());
            values.add(value == null ? null : String.valueOf(value));
        }
        return values;
    }

    public List<AggregateRow> distinctRows(ValkeyFieldReference... fields) {
        return distinctRows(0, 1_000, fields);
    }

    public List<AggregateRow> distinctRows(int offset, int limit, ValkeyFieldReference... fields) {
        return distinctResult(offset, limit, fields).rows();
    }

    public <R> List<R> distinctRows(Function<AggregateRow, R> mapper, ValkeyFieldReference... fields) {
        return distinctRows(0, 1_000, mapper, fields);
    }

    public <R> List<R> distinctRows(int offset, int limit, Function<AggregateRow, R> mapper, ValkeyFieldReference... fields) {
        Objects.requireNonNull(mapper, "mapper");
        List<AggregateRow> rows = distinctRows(offset, limit, fields);
        List<R> mapped = new java.util.ArrayList<>(rows.size());
        for (AggregateRow row : rows) {
            mapped.add(mapper.apply(row));
        }
        return mapped;
    }

    public <R> List<R> distinctRows(Class<R> type, ValkeyFieldReference... fields) {
        return distinctRows(0, 1_000, type, fields);
    }

    public <R> List<R> distinctRows(int offset, int limit, Class<R> type, ValkeyFieldReference... fields) {
        Objects.requireNonNull(type, "type");
        return distinctRows(offset, limit, row -> row.toObject(type), fields);
    }

    private AggregateResult distinctResult(int offset, int limit, ValkeyFieldReference... fields) {
        if (vectorQuery != null) {
            throw new UnsupportedOperationException("distinct() is not supported for vector KNN queries");
        }
        List<String> fieldNames = requireDistinctFields(fields);
        AggregateRequest request = new AggregateRequest(
                fieldNames,
                List.of(),
                null,
                true,
                Math.max(0, offset),
                limit <= 0 ? 1_000 : limit
        );
        return repository.aggregate(buildFinalCondition(), request);
    }

    private List<String> requireDistinctFields(ValkeyFieldReference... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("Distinct fields cannot be empty");
        }
        List<String> names = new java.util.ArrayList<>(fields.length);
        for (ValkeyFieldReference field : fields) {
            if (field == null || field.fieldName() == null || field.fieldName().isBlank()) {
                throw new IllegalArgumentException("Distinct field cannot be blank");
            }
            if (!names.contains(field.fieldName())) {
                names.add(field.fieldName());
            }
        }
        return List.copyOf(names);
    }
}
