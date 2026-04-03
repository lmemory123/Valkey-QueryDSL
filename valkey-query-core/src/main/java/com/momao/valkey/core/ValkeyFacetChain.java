package com.momao.valkey.core;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

public final class ValkeyFacetChain<T> {

    private static final int DEFAULT_SCAN_PAGE_SIZE = 100;

    private final ValkeyRepository<T> repository;
    private SearchCondition condition;
    private final List<String> fieldNames = new ArrayList<>();
    private String countAlias = "count";
    private String sortField = "count";
    private boolean sortAscending = false;
    private int offset;
    private int limit = 10;
    private long minCount = 1L;
    private Set<String> includedValues = Set.of();

    public ValkeyFacetChain(ValkeyRepository<T> repository) {
        this.repository = repository;
    }

    public ValkeyFacetChain<T> where(SearchCondition condition) {
        this.condition = condition;
        return this;
    }

    public ValkeyFacetChain<T> and(SearchCondition nextCondition) {
        if (this.condition == null) {
            this.condition = nextCondition;
        } else {
            this.condition = this.condition.and(nextCondition);
        }
        return this;
    }

    public ValkeyFacetChain<T> or(SearchCondition nextCondition) {
        if (this.condition == null) {
            this.condition = nextCondition;
        } else {
            this.condition = this.condition.or(nextCondition);
        }
        return this;
    }

    public ValkeyFacetChain<T> andIf(boolean execute, Supplier<SearchCondition> conditionSupplier) {
        if (execute) {
            and(conditionSupplier.get());
        }
        return this;
    }

    public ValkeyFacetChain<T> orIf(boolean execute, Supplier<SearchCondition> conditionSupplier) {
        if (execute) {
            or(conditionSupplier.get());
        }
        return this;
    }

    public ValkeyFacetChain<T> on(ValkeyFieldReference field) {
        fieldNames.clear();
        addField(field);
        return this;
    }

    public ValkeyFacetChain<T> on(ValkeyFieldReference... fields) {
        fieldNames.clear();
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("Facet fields cannot be empty");
        }
        for (ValkeyFieldReference field : fields) {
            addField(field);
        }
        return this;
    }

    private void addField(ValkeyFieldReference field) {
        if (field == null || field.fieldName() == null || field.fieldName().isBlank()) {
            throw new IllegalArgumentException("Facet field cannot be blank");
        }
        if (!fieldNames.contains(field.fieldName())) {
            fieldNames.add(field.fieldName());
        }
    }

    public ValkeyFacetChain<T> size(int limit) {
        this.limit = limit <= 0 ? 10 : limit;
        return this;
    }

    public ValkeyFacetChain<T> offset(int offset) {
        this.offset = Math.max(0, offset);
        return this;
    }

    public ValkeyFacetChain<T> countAlias(String alias) {
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("Facet count alias cannot be blank");
        }
        this.countAlias = alias;
        if ("count".equals(sortField) || sortField.equals(this.countAlias)) {
            this.sortField = alias;
        }
        return this;
    }

    public ValkeyFacetChain<T> minCount(long minCount) {
        if (minCount < 1L) {
            throw new IllegalArgumentException("Facet minCount must be >= 1");
        }
        this.minCount = minCount;
        return this;
    }

    public ValkeyFacetChain<T> includeValues(String... values) {
        if (values == null || values.length == 0) {
            this.includedValues = Set.of();
            return this;
        }
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                normalized.add(value);
            }
        }
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Facet includeValues cannot be empty");
        }
        this.includedValues = Set.copyOf(normalized);
        return this;
    }

    public ValkeyFacetChain<T> sortByCountDesc() {
        this.sortField = countAlias;
        this.sortAscending = false;
        return this;
    }

    public ValkeyFacetChain<T> sortByCountAsc() {
        this.sortField = countAlias;
        this.sortAscending = true;
        return this;
    }

    public ValkeyFacetChain<T> sortByValueAsc() {
        this.sortField = requireSingleField();
        this.sortAscending = true;
        return this;
    }

    public ValkeyFacetChain<T> sortByValueDesc() {
        this.sortField = requireSingleField();
        this.sortAscending = false;
        return this;
    }

    public FacetResult result() {
        String targetField = requireSingleField();
        return executeFacet(targetField);
    }

    public FacetResults results() {
        List<String> targets = requireFields();
        if (targets.size() > 1 && !includedValues.isEmpty()) {
            throw new IllegalStateException("Facet includeValues requires exactly one field; use result() for single-field facets");
        }
        List<FacetResult> facets = new ArrayList<>(targets.size());
        for (String targetField : targets) {
            facets.add(executeFacet(targetField));
        }
        return new FacetResults(facets);
    }

    private FacetResult executeFacet(String targetField) {
        if (minCount <= 1L && includedValues.isEmpty()) {
            AggregateResult aggregate = repository.aggregate(
                    condition != null ? condition : new SearchCondition("*"),
                    new AggregateRequest(
                            List.of(targetField),
                            List.of(AggregateReducer.count(countAlias)),
                            sortField,
                            sortAscending,
                            offset,
                            limit
                    )
            );
            return aggregate.toFacet(targetField, countAlias);
        }
        return executeFilteredFacet(targetField);
    }

    private FacetResult executeFilteredFacet(String targetField) {
        SearchCondition finalCondition = condition != null ? condition : new SearchCondition("*");
        int scanOffset = 0;
        int scanLimit = Math.max(DEFAULT_SCAN_PAGE_SIZE, offset + limit);
        List<FacetBucket> pageBuckets = new ArrayList<>();
        long filteredTotal = 0L;
        while (true) {
            AggregateResult aggregate = repository.aggregate(
                    finalCondition,
                    new AggregateRequest(
                            List.of(targetField),
                            List.of(AggregateReducer.count(countAlias)),
                            sortField,
                            sortAscending,
                            scanOffset,
                            scanLimit
                    )
            );
            List<AggregateRow> rows = aggregate.rows();
            for (AggregateRow row : rows) {
                String value = row.getString(targetField);
                Long count = row.getLong(countAlias);
                long bucketCount = count == null ? 0L : count;
                if (bucketCount < minCount || !matchesIncludedValue(value)) {
                    continue;
                }
                filteredTotal++;
                if (filteredTotal > offset && pageBuckets.size() < limit) {
                    pageBuckets.add(new FacetBucket(value, bucketCount));
                }
            }
            if (rows.size() < scanLimit) {
                break;
            }
            scanOffset += scanLimit;
        }
        return new FacetResult(targetField, filteredTotal, pageBuckets);
    }

    private boolean matchesIncludedValue(String value) {
        return includedValues.isEmpty() || includedValues.contains(value);
    }

    private String requireSingleField() {
        List<String> fields = requireFields();
        if (fields.size() != 1) {
            throw new IllegalStateException("Facet result() requires exactly one field; use results() for multi-field facets");
        }
        return fields.get(0);
    }

    private List<String> requireFields() {
        if (fieldNames.isEmpty()) {
            throw new IllegalStateException("Facet field has not been configured");
        }
        return List.copyOf(fieldNames);
    }
}
