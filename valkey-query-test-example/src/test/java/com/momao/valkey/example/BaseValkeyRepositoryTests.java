package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseValkeyRepositoryTests {

    @Test
    void metadataIsGenerated() {
        assertNotNull(StudentQuery.METADATA);
        assertEquals("idx:student", StudentQuery.METADATA.indexName());
        assertEquals(StorageType.HASH, StudentQuery.METADATA.storageType());
        assertEquals("student:", StudentQuery.METADATA.prefix());
        assertEquals(7, StudentQuery.METADATA.fields().size());

        assertEquals("idx:sku", SkuQuery.METADATA.indexName());
        assertEquals(StorageType.JSON, SkuQuery.METADATA.storageType());
        assertEquals("sku:", SkuQuery.METADATA.prefix());
        assertEquals(5, SkuQuery.METADATA.fields().size());
        assertEquals("merchant_name", SkuQuery.METADATA.fields().get(4).fieldName());
        assertEquals("merchant.name", SkuQuery.METADATA.fields().get(4).jsonPath());
    }

    @Test
    void repositoryUsesMetadata() {
        StudentRepository repo = new StudentRepository();
        assertEquals("idx:student", repo.getIndexName());
        assertEquals("student:", repo.getPrefix());
        assertEquals(1, repo.getPrefixes().size());
        assertEquals(7, repo.getFields().size());
    }

    @Test
    void mapsHashSearchResultToEntityRecords() {
        StudentRepository repo = new StudentRepository();
        Object[] response = new Object[]{
                1L,
                Map.of(
                        "student:1",
                        Map.of(
                                "name", "Alice",
                                "age", "18",
                                "score", "95.5",
                                "class_name", "Class A",
                                "department", "Backend",
                                "status", "ON"
                        )
                )
        };

        SearchResult<Student> result = repo.map(response);

        assertEquals(1L, result.total());
        assertEquals(1, result.records().size());
        Student student = result.records().get(0);
        assertEquals(1L, student.getId());
        assertEquals("Alice", student.getName());
        assertEquals(18, student.getAge());
        assertEquals(95.5d, student.getScore());
        assertEquals("Class A", student.getClassName());
        assertEquals("Backend", student.getDepartment());
    }

    @Test
    void mapsJsonSearchResultToEntityRecords() {
        JsonSkuRepository repo = new JsonSkuRepository();
        Object[] response = new Object[]{
                1L,
                Map.of(
                        "sku:1",
                        Map.of(
                                "$", "{\"title\":\"Phone Pro\",\"price\":2999,\"tags\":[\"HOT\",\"NEW\"],\"merchant\":{\"name\":\"Apple Store\",\"level\":\"S\"}}"
                        )
                )
        };

        SearchResult<Sku> result = repo.map(response);

        assertEquals(1L, result.total());
        assertEquals(1, result.records().size());
        Sku sku = result.records().get(0);
        assertEquals("1", sku.getId());
        assertEquals("Phone Pro", sku.getTitle());
        assertEquals(2999, sku.getPrice());
        assertEquals(List.of("HOT", "NEW"), sku.getTags());
        assertEquals("Apple Store", sku.getMerchant().getName());
        assertEquals("S", sku.getMerchant().getLevel());
    }

    @Test
    void convertsEntityToHashMapForSaving() {
        StudentRepository repo = new StudentRepository();
        Student student = new Student();
        student.setId(1L);
        student.setName("Alice");
        student.setAge(18);
        student.setScore(95.5d);
        student.setClassName("Class A");
        student.setDepartment("Backend");
        student.setStatus("ON");

        Map<String, String> mapped = repo.asStringMap(student);

        assertEquals("1", mapped.get("id"));
        assertEquals("Alice", mapped.get("name"));
        assertEquals("18", mapped.get("age"));
        assertEquals("95.5", mapped.get("score"));
        assertEquals("Class A", mapped.get("class_name"));
        assertEquals("Backend", mapped.get("department"));
        assertEquals("ON", mapped.get("status"));
        assertTrue(!mapped.containsKey("unknown"));
    }

    @Test
    void buildsSortedSearchCommandWithServerSortAndPageWindow() {
        StudentRepository repo = new StudentRepository();

        String[] command = repo.searchCommand(new SearchCondition("@name:Alice").sortBy("score", true), 20, 10);

        assertEquals("FT.SEARCH", command[0]);
        assertEquals("idx:student", command[1]);
        assertEquals("@name:Alice", command[2]);
        assertEquals("SORTBY", command[3]);
        assertEquals("score", command[4]);
        assertEquals("ASC", command[5]);
        assertEquals("LIMIT", command[6]);
        assertEquals("20", command[7]);
        assertEquals("10", command[8]);
    }

    static class StudentRepository extends BaseValkeyRepository<Student> {

        StudentRepository() {
            super(StudentQuery.METADATA, null, Student.class, new ObjectMapper().findAndRegisterModules());
        }

        SearchResult<Student> map(Object[] response) {
            return mapSearchResponse(response);
        }

        Map<String, String> asStringMap(Student student) {
            return toStringMap(student);
        }

        String[] searchCommand(SearchCondition condition, int offset, int limit) {
            return buildSearchCommand(condition, offset, limit);
        }
    }

    static class JsonSkuRepository extends BaseValkeyRepository<Sku> {

        JsonSkuRepository() {
            super(new IndexSchema(
                    "idx:sku-json",
                    StorageType.JSON,
                    List.of("sku:"),
                    List.of(
                            SchemaField.tag("id", ",", true),
                            SchemaField.text("title", 2.5d, true, false),
                            SchemaField.numeric("price", true),
                            SchemaField.tag("tags", "tags[*]", ",", false),
                            SchemaField.tag("merchant", "merchant.name", ",", false)
                    )
            ), null, Sku.class, new ObjectMapper().findAndRegisterModules());
        }

        SearchResult<Sku> map(Object[] response) {
            return mapSearchResponse(response);
        }
    }
}
