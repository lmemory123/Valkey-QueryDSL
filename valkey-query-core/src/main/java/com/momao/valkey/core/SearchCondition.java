package com.momao.valkey.core;

import java.util.ArrayList;
import java.util.List;

public final class SearchCondition {

    private final String expression;

    private final String sortField;

    private final boolean sortAscending;

    private final SearchPredicate predicate;

    private final List<String> selectedFields;

    private final VectorQuery vectorQuery;

    public SearchCondition(String expression) {
        this(expression, null, true, null, List.of(), null);
    }

    static SearchCondition textEquals(String fieldName, String value) {
        return new SearchCondition(
                "@" + fieldName + ":" + ValkeySyntaxUtils.escape(value),
                null,
                true,
                new SearchPredicate.ExactMatch(fieldName, value),
                List.of(),
                null
        );
    }

    static SearchCondition tagEquals(String fieldName, String value) {
        return new SearchCondition(
                "@" + fieldName + ":{" + ValkeySyntaxUtils.escapeTag(value) + "}",
                null,
                true,
                new SearchPredicate.ExactMatch(fieldName, value),
                List.of(),
                null
        );
    }

    static SearchCondition numericEquals(String fieldName, Number value) {
        return new SearchCondition(
                "@" + fieldName + ":[" + value + " " + value + "]",
                null,
                true,
                new SearchPredicate.ExactMatch(fieldName, value),
                List.of(),
                null
        );
    }

    private SearchCondition(
            String expression,
            String sortField,
            boolean sortAscending,
            SearchPredicate predicate,
            List<String> selectedFields,
            VectorQuery vectorQuery) {
        this.expression = expression;
        this.sortField = sortField;
        this.sortAscending = sortAscending;
        this.predicate = predicate;
        this.selectedFields = selectedFields == null ? List.of() : List.copyOf(selectedFields);
        this.vectorQuery = vectorQuery;
    }

    public SearchCondition and(SearchCondition other) {
        SearchPredicate merged = predicate != null && other.predicate != null
                ? new SearchPredicate.And(predicate, other.predicate)
                : null;
        return new SearchCondition("(" + expression + " " + other.expression + ")", null, true, merged, mergeProjection(other), mergeVectorQuery(other));
    }

    public SearchCondition or(SearchCondition other) {
        SearchPredicate merged = predicate != null && other.predicate != null
                ? new SearchPredicate.Or(predicate, other.predicate)
                : null;
        return new SearchCondition("(" + expression + " | " + other.expression + ")", null, true, merged, mergeProjection(other), mergeVectorQuery(other));
    }

    public SearchCondition not() {
        return new SearchCondition("-(" + expression + ")", sortField, sortAscending, null, selectedFields, vectorQuery);
    }

    public SearchCondition sortBy(String fieldName) {
        return sortBy(fieldName, true);
    }

    public SearchCondition sortBy(String fieldName, boolean ascending) {
        return new SearchCondition(expression, fieldName, ascending, predicate, selectedFields, vectorQuery);
    }

    public SearchCondition project(List<String> fields) {
        return new SearchCondition(expression, sortField, sortAscending, predicate, normalizeProjection(fields), vectorQuery);
    }

    public SearchCondition withVectorQuery(VectorQuery vectorQuery) {
        return new SearchCondition(expression, sortField, sortAscending, predicate, selectedFields, vectorQuery);
    }

    public String build() {
        return expression;
    }

    public boolean hasSort() {
        return sortField != null && !sortField.isBlank();
    }

    public String sortField() {
        return sortField;
    }

    public boolean sortAscending() {
        return sortAscending;
    }

    public boolean hasProjection() {
        return !selectedFields.isEmpty();
    }

    public List<String> selectedFields() {
        return selectedFields;
    }

    public boolean hasVectorQuery() {
        return vectorQuery != null;
    }

    public VectorQuery vectorQuery() {
        return vectorQuery;
    }

    SearchPredicate predicate() {
        return predicate;
    }

    private List<String> normalizeProjection(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return List.of();
        }
        List<String> normalized = new ArrayList<>();
        for (String field : fields) {
            if (field == null) {
                continue;
            }
            String trimmed = field.trim();
            if (!trimmed.isEmpty() && !normalized.contains(trimmed)) {
                normalized.add(trimmed);
            }
        }
        return normalized;
    }

    private List<String> mergeProjection(SearchCondition other) {
        if (!selectedFields.isEmpty()) {
            return selectedFields;
        }
        return other == null ? List.of() : other.selectedFields;
    }

    private VectorQuery mergeVectorQuery(SearchCondition other) {
        if (vectorQuery == null) {
            return other == null ? null : other.vectorQuery;
        }
        if (other == null || other.vectorQuery == null) {
            return vectorQuery;
        }
        if (vectorQuery.equals(other.vectorQuery)) {
            return vectorQuery;
        }
        throw new IllegalArgumentException("Only one vector KNN query is supported in a SearchCondition");
    }
}
