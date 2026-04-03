package com.momao.valkey.core;

public sealed interface SearchPredicate permits SearchPredicate.ExactMatch, SearchPredicate.And, SearchPredicate.Or {

    record ExactMatch(String fieldName, Object expectedValue) implements SearchPredicate {

        public ExactMatch {
            if (fieldName == null || fieldName.isBlank()) {
                throw new IllegalArgumentException("Field name cannot be blank");
            }
            if (expectedValue == null) {
                throw new IllegalArgumentException("Expected value cannot be null");
            }
        }
    }

    record And(SearchPredicate left, SearchPredicate right) implements SearchPredicate {

        public And {
            if (left == null || right == null) {
                throw new IllegalArgumentException("And predicate cannot contain null");
            }
        }
    }

    record Or(SearchPredicate left, SearchPredicate right) implements SearchPredicate {

        public Or {
            if (left == null || right == null) {
                throw new IllegalArgumentException("Or predicate cannot contain null");
            }
        }
    }
}
