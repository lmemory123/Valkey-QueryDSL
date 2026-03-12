package com.momao.valkey.adapter;

import com.momao.valkey.core.metadata.SchemaField;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonEntityOperationsTests {

    private final JsonEntityOperations<Object> operations = new JsonEntityOperations<>();

    @Test
    void usesRealJsonPropertyPathAndAliasWhenBuildingFieldInfo() {
        FieldInfo fieldInfo = operations.toFieldInfo(SchemaField.numeric("play_count", "playCount", true));

        assertEquals(List.of("$.playCount", "AS", "play_count", "NUMERIC"), stringify(fieldInfo));
    }

    @Test
    void keepsExplicitJsonCollectionPathForTagFields() {
        FieldInfo fieldInfo = operations.toFieldInfo(SchemaField.tag("producer_mark", "producerMark[*]", ",", false));

        assertEquals(List.of("$.producerMark[*]", "AS", "producer_mark", "TAG"), stringify(fieldInfo));
    }

    private List<String> stringify(FieldInfo fieldInfo) {
        return Arrays.stream(fieldInfo.toArgs())
                .map(Object::toString)
                .toList();
    }
}