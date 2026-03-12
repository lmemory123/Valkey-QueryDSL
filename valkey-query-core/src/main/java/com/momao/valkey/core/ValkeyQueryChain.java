package com.momao.valkey.core;

import java.util.List;
import java.util.function.Supplier;

public class ValkeyQueryChain<T> {

    private final ValkeyRepository<T> repository;
    private SearchCondition condition;

    private String sortField;
    private boolean sortAscending = true;

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

    SearchCondition buildFinalCondition() {
        SearchCondition finalCond = this.condition != null ? this.condition : new SearchCondition("*");
        if (this.sortField != null) {
            finalCond = finalCond.sortBy(this.sortField, this.sortAscending);
        }
        return finalCond;
    }

    public List<T> list() {
        return repository.list(buildFinalCondition());
    }

    public Page<T> page(int offset, int limit) {
        return repository.page(buildFinalCondition(), offset, limit);
    }

    public T one() {
        return repository.one(buildFinalCondition());
    }

    public long count() {
        return repository.count(buildFinalCondition());
    }
}