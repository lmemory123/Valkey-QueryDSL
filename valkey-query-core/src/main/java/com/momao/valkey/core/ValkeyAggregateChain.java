package com.momao.valkey.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public final class ValkeyAggregateChain<T> {

    private final ValkeyRepository<T> repository;
    private SearchCondition condition;
    private final List<String> groupByFields = new ArrayList<>();
    private final List<AggregateReducer> reducers = new ArrayList<>();
    private final List<AggregateApply> applies = new ArrayList<>();
    private final List<String> filters = new ArrayList<>();
    private String sortField;
    private boolean sortAscending = true;
    private int offset;
    private int limit = 1_000;

    public ValkeyAggregateChain(ValkeyRepository<T> repository) {
        this.repository = repository;
    }

    public ValkeyAggregateChain<T> where(SearchCondition condition) {
        this.condition = condition;
        return this;
    }

    public ValkeyAggregateChain<T> and(SearchCondition nextCondition) {
        if (this.condition == null) {
            this.condition = nextCondition;
        } else {
            this.condition = this.condition.and(nextCondition);
        }
        return this;
    }

    public ValkeyAggregateChain<T> or(SearchCondition nextCondition) {
        if (this.condition == null) {
            this.condition = nextCondition;
        } else {
            this.condition = this.condition.or(nextCondition);
        }
        return this;
    }

    public ValkeyAggregateChain<T> andIf(boolean execute, Supplier<SearchCondition> conditionSupplier) {
        if (execute) {
            and(conditionSupplier.get());
        }
        return this;
    }

    public ValkeyAggregateChain<T> orIf(boolean execute, Supplier<SearchCondition> conditionSupplier) {
        if (execute) {
            or(conditionSupplier.get());
        }
        return this;
    }

    public ValkeyAggregateChain<T> groupBy(ValkeyFieldReference... fields) {
        if (fields == null) {
            return this;
        }
        for (ValkeyFieldReference field : fields) {
            if (field == null || field.fieldName() == null || field.fieldName().isBlank()) {
                continue;
            }
            if (!groupByFields.contains(field.fieldName())) {
                groupByFields.add(field.fieldName());
            }
        }
        return this;
    }

    public ValkeyAggregateChain<T> count() {
        return count("count");
    }

    public ValkeyAggregateChain<T> count(String alias) {
        reducers.add(AggregateReducer.count(alias));
        return this;
    }

    public ValkeyAggregateChain<T> countDistinct(ValkeyFieldReference field) {
        return countDistinct(field, field.fieldName() + "_count_distinct");
    }

    public ValkeyAggregateChain<T> countDistinct(ValkeyFieldReference field, String alias) {
        if (field == null || field.fieldName() == null || field.fieldName().isBlank()) {
            throw new IllegalArgumentException("Aggregate countDistinct field cannot be blank");
        }
        reducers.add(AggregateReducer.countDistinct(field.fieldName(), alias));
        return this;
    }

    public ValkeyAggregateChain<T> sum(NumericFieldBuilder field) {
        return sum(field, field.fieldName() + "_sum");
    }

    public ValkeyAggregateChain<T> sum(NumericFieldBuilder field, String alias) {
        reducers.add(AggregateReducer.sum(field.fieldName(), alias));
        return this;
    }

    public ValkeyAggregateChain<T> avg(NumericFieldBuilder field) {
        return avg(field, field.fieldName() + "_avg");
    }

    public ValkeyAggregateChain<T> avg(NumericFieldBuilder field, String alias) {
        reducers.add(AggregateReducer.avg(field.fieldName(), alias));
        return this;
    }

    public ValkeyAggregateChain<T> min(NumericFieldBuilder field) {
        return min(field, field.fieldName() + "_min");
    }

    public ValkeyAggregateChain<T> min(NumericFieldBuilder field, String alias) {
        reducers.add(AggregateReducer.min(field.fieldName(), alias));
        return this;
    }

    public ValkeyAggregateChain<T> max(NumericFieldBuilder field) {
        return max(field, field.fieldName() + "_max");
    }

    public ValkeyAggregateChain<T> max(NumericFieldBuilder field, String alias) {
        reducers.add(AggregateReducer.max(field.fieldName(), alias));
        return this;
    }

    public ValkeyAggregateChain<T> apply(String expression, String alias) {
        applies.add(new AggregateApply(expression, alias));
        return this;
    }

    public ValkeyAggregateChain<T> apply(AggregateExpression expression, String alias) {
        if (expression == null) {
            throw new IllegalArgumentException("Aggregate apply expression cannot be null");
        }
        return apply(expression.render(), alias);
    }

    public ValkeyAggregateChain<T> filter(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Aggregate filter expression cannot be blank");
        }
        filters.add(expression);
        return this;
    }

    public ValkeyAggregateChain<T> filter(AggregatePredicate predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("Aggregate filter predicate cannot be null");
        }
        return filter(predicate.render());
    }

    public ValkeyAggregateChain<T> sortByAsc(String field) {
        this.sortField = field;
        this.sortAscending = true;
        return this;
    }

    public ValkeyAggregateChain<T> sortByDesc(String field) {
        this.sortField = field;
        this.sortAscending = false;
        return this;
    }

    public ValkeyAggregateChain<T> limit(int offset, int limit) {
        this.offset = Math.max(0, offset);
        this.limit = limit <= 0 ? 1_000 : limit;
        return this;
    }

    public AggregateResult result() {
        return repository.aggregate(condition != null ? condition : new SearchCondition("*"), buildRequest());
    }

    public List<AggregateRow> list() {
        return result().rows();
    }

    AggregateRequest buildRequest() {
        return new AggregateRequest(groupByFields, reducers, applies, filters, sortField, sortAscending, offset, limit);
    }
}
