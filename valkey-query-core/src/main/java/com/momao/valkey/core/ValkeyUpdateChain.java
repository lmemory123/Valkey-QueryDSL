package com.momao.valkey.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public final class ValkeyUpdateChain<T> {

    private static final Object NO_ID = new Object();

    private final ValkeyRepository<T> repository;

    private final List<UpdateOperation> operations = new ArrayList<>();

    private Object id;

    private SearchPredicate predicate;

    public ValkeyUpdateChain(ValkeyRepository<T> repository) {
        this.repository = repository;
    }

    public ValkeyUpdateChain<T> set(ValkeyFieldReference field, Object value) {
        if (field == null) {
            throw new IllegalArgumentException("Field reference cannot be null");
        }
        return set(field.fieldName(), value);
    }

    public ValkeyUpdateChain<T> set(String fieldName, Object value) {
        operations.add(new UpdateAssignment(fieldName, value));
        return this;
    }

    public ValkeyUpdateChain<T> increment(NumericFieldBuilder field, Number delta) {
        return numericOperation(field, delta, UpdateOperationKind.INCREMENT);
    }

    public ValkeyUpdateChain<T> decrement(NumericFieldBuilder field, Number delta) {
        return numericOperation(field, delta, UpdateOperationKind.DECREMENT);
    }

    public ValkeyUpdateChain<T> setIf(boolean execute, ValkeyFieldReference field, Object value) {
        if (execute) {
            set(field, value);
        }
        return this;
    }

    public ValkeyUpdateChain<T> setIf(boolean execute, String fieldName, Object value) {
        if (execute) {
            set(fieldName, value);
        }
        return this;
    }

    public ValkeyUpdateChain<T> incrementIf(boolean execute, NumericFieldBuilder field, Number delta) {
        if (execute) {
            increment(field, delta);
        }
        return this;
    }

    public ValkeyUpdateChain<T> decrementIf(boolean execute, NumericFieldBuilder field, Number delta) {
        if (execute) {
            decrement(field, delta);
        }
        return this;
    }

    public ValkeyUpdateChain<T> where(SearchCondition condition) {
        this.predicate = extractPredicate(condition);
        return this;
    }

    public ValkeyUpdateChain<T> and(SearchCondition condition) {
        SearchPredicate next = extractPredicate(condition);
        this.predicate = this.predicate == null ? next : new SearchPredicate.And(this.predicate, next);
        return this;
    }

    public ValkeyUpdateChain<T> or(SearchCondition condition) {
        SearchPredicate next = extractPredicate(condition);
        this.predicate = this.predicate == null ? next : new SearchPredicate.Or(this.predicate, next);
        return this;
    }

    public ValkeyUpdateChain<T> andIf(boolean execute, Supplier<SearchCondition> supplier) {
        if (execute) {
            and(supplier.get());
        }
        return this;
    }

    public ValkeyUpdateChain<T> orIf(boolean execute, Supplier<SearchCondition> supplier) {
        if (execute) {
            or(supplier.get());
        }
        return this;
    }

    public ValkeyUpdateChain<T> expect(ValkeyFieldReference field, Object expectedValue) {
        if (field == null) {
            throw new IllegalArgumentException("Field reference cannot be null");
        }
        return andPredicate(new SearchPredicate.ExactMatch(field.fieldName(), expectedValue));
    }

    public ValkeyUpdateChain<T> expect(String fieldName, Object expectedValue) {
        return andPredicate(new SearchPredicate.ExactMatch(fieldName, expectedValue));
    }

    public ValkeyUpdateChain<T> expectVersion(ValkeyFieldReference field, Number expectedVersion) {
        validateVersionField(field);
        return expect(field, expectedVersion);
    }

    public ValkeyUpdateChain<T> advanceVersion(ValkeyFieldReference field, Number expectedVersion) {
        validateVersionField(field);
        if (expectedVersion == null) {
            throw new IllegalArgumentException("Expected version cannot be null");
        }
        expectVersion(field, expectedVersion);
        set(field, nextVersion(expectedVersion));
        return this;
    }

    public ValkeyUpdateChain<T> whereId(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        this.id = id;
        return this;
    }

    public long execute() {
        if (id == null) {
            if (containsOr(predicate)) {
                throw new IllegalStateException("UpdateChain cannot infer id from predicates containing OR; use whereId(...) explicitly");
            }
            Object inferred = extractId(predicate);
            id = inferred == NO_ID ? null : inferred;
        }
        if (id == null) {
            throw new IllegalStateException("where(...) must include q.id.eq(...) or use whereId(...) before execute()");
        }
        if (operations.isEmpty()) {
            throw new IllegalStateException("At least one update operation is required");
        }
        return repository.updateById(id, List.copyOf(operations), predicate);
    }

    private ValkeyUpdateChain<T> andPredicate(SearchPredicate next) {
        this.predicate = this.predicate == null ? next : new SearchPredicate.And(this.predicate, next);
        return this;
    }

    private SearchPredicate extractPredicate(SearchCondition condition) {
        if (condition == null) {
            throw new IllegalArgumentException("Condition cannot be null");
        }
        SearchPredicate extracted = condition.predicate();
        if (extracted == null) {
            throw new IllegalArgumentException("UpdateChain only supports structured equality predicates in where/and/or");
        }
        return extracted;
    }

    private Object extractId(SearchPredicate source) {
        if (source == null) {
            return NO_ID;
        }
        if (source instanceof SearchPredicate.ExactMatch exactMatch) {
            return "id".equals(exactMatch.fieldName()) ? exactMatch.expectedValue() : NO_ID;
        }
        if (source instanceof SearchPredicate.And and) {
            Object left = extractId(and.left());
            Object right = extractId(and.right());
            if (left != NO_ID && right != NO_ID && !left.equals(right)) {
                throw new IllegalStateException("UpdateChain found conflicting id predicates; use whereId(...) explicitly");
            }
            return left != NO_ID ? left : right;
        }
        return NO_ID;
    }

    private boolean containsOr(SearchPredicate source) {
        if (source == null) {
            return false;
        }
        if (source instanceof SearchPredicate.Or) {
            return true;
        }
        if (source instanceof SearchPredicate.And and) {
            return containsOr(and.left()) || containsOr(and.right());
        }
        return false;
    }

    private void validateVersionField(ValkeyFieldReference field) {
        if (field == null) {
            throw new IllegalArgumentException("Field reference cannot be null");
        }
        String fieldName = field.fieldName();
        String normalized = fieldName == null ? "" : fieldName.toLowerCase();
        if (!("version".equals(normalized) || normalized.endsWith(".version") || normalized.endsWith("_version"))) {
            throw new IllegalArgumentException("expectVersion/advanceVersion only supports version-like fields");
        }
    }

    private Object nextVersion(Number currentVersion) {
        if (currentVersion instanceof Long || currentVersion instanceof java.util.concurrent.atomic.AtomicLong) {
            return currentVersion.longValue() + 1L;
        }
        if (currentVersion instanceof Integer || currentVersion instanceof Short || currentVersion instanceof Byte) {
            return currentVersion.intValue() + 1;
        }
        return currentVersion.longValue() + 1L;
    }

    private ValkeyUpdateChain<T> numericOperation(NumericFieldBuilder field, Number delta, UpdateOperationKind kind) {
        if (field == null) {
            throw new IllegalArgumentException("Field reference cannot be null");
        }
        operations.add(new NumericUpdateOperation(field.fieldName(), delta, kind));
        return this;
    }
}
