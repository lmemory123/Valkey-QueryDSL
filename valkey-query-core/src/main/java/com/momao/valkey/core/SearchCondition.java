package com.momao.valkey.core;

public final class SearchCondition {

    private final String expression;

    private final String sortField;

    private final boolean sortAscending;

    public SearchCondition(String expression) {
        this(expression, null, true);
    }

    private SearchCondition(String expression, String sortField, boolean sortAscending) {
        this.expression = expression;
        this.sortField = sortField;
        this.sortAscending = sortAscending;
    }

    public SearchCondition and(SearchCondition other) {
        return new SearchCondition("(" + expression + " " + other.expression + ")");
    }

    public SearchCondition or(SearchCondition other) {
        return new SearchCondition("(" + expression + " | " + other.expression + ")");
    }

    public SearchCondition not() {
        return new SearchCondition("-(" + expression + ")", sortField, sortAscending);
    }

    public SearchCondition sortBy(String fieldName) {
        return sortBy(fieldName, true);
    }

    public SearchCondition sortBy(String fieldName, boolean ascending) {
        return new SearchCondition(expression, fieldName, ascending);
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
}
