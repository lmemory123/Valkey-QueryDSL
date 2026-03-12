package com.momao.valkey.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SearchConditionTests {

    @Test
    void combinesConditionsWithParentheses() {
        SearchCondition condition = new SearchCondition("@name:Alice")
                .and(new SearchCondition("@age:[18 25]"));

        assertEquals("(@name:Alice @age:[18 25])", condition.build());
    }

    @Test
    void escapesReservedCharacters() {
        assertEquals("hello\\-world\\!", ValkeySyntaxUtils.escape("hello-world!"));
    }

    @Test
    void carriesSortMetadata() {
        SearchCondition condition = new SearchCondition("@price:[1 100]").sortBy("price", false);

        assertEquals("@price:[1 100]", condition.build());
        assertTrue(condition.hasSort());
        assertEquals("price", condition.sortField());
        assertFalse(condition.sortAscending());
    }
}
