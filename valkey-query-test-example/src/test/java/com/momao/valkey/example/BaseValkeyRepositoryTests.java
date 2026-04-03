package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.DefaultValkeyClientRouting;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import com.momao.valkey.core.AggregateReducer;
import com.momao.valkey.core.AggregateApply;
import com.momao.valkey.core.AggregateRequest;
import com.momao.valkey.core.AggregateResult;
import com.momao.valkey.core.AggregateRow;
import com.momao.valkey.core.FacetResult;
import com.momao.valkey.core.BulkSaveItem;
import com.momao.valkey.core.BulkWriteOptions;
import com.momao.valkey.core.BulkWriteResult;
import com.momao.valkey.core.Page;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.exception.ValkeyConfigurationException;
import com.momao.valkey.core.exception.ValkeyIndexException;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import com.momao.valkey.core.exception.ValkeyResultMappingException;
import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.ValkeyFieldReference;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                                "$", "{\"title\":\"Phone Pro\",\"price\":2999,\"tags\":\"HOT,NEW\",\"merchant\":{\"name\":\"Apple Store\",\"level\":\"S\"}}"
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
    void mapsJsonProjectionResultToEntityRecords() {
        JsonSkuRepository repo = new JsonSkuRepository();
        Object[] response = new Object[]{
                1L,
                Map.of(
                        "sku:1",
                        Map.of(
                                "title", "\"Phone Pro\"",
                                "price", "2999",
                                "merchant", "\"Apple Store\""
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
        assertEquals("Apple Store", sku.getMerchant().getName());
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

    @Test
    void buildsCreateCommandWithSuffixTrieForTextFields() {
        StudentRepository repo = new StudentRepository();

        String[] command = repo.createCommand();

        assertEquals("FT.CREATE", command[0]);
        assertEquals("idx:student", command[1]);
        assertTrue(List.of(command).contains("SCHEMA"));
        assertTrue(List.of(command).contains("WITHSUFFIXTRIE"));
        assertTrue(List.of(command).contains("TEXT"));
    }

    @Test
    void buildsJsonCreateCommandWithCollectionTagCompatibilityPath() {
        JsonSkuRepository repo = new JsonSkuRepository();

        String[] command = repo.createCommand();
        List<String> tokens = List.of(command);

        assertTrue(tokens.contains("$.tags"));
        assertTrue(!tokens.contains("$.tags[*]"));
        assertTrue(tokens.contains("SEPARATOR"));
    }

    @Test
    void buildsHashSearchCommandWithProjectionFields() {
        StudentRepository repo = new StudentRepository();

        String[] command = repo.searchCommand(new SearchCondition("@name:Alice").project(List.of("name", "score")), 0, 10);

        assertEquals(List.of(
                "FT.SEARCH", "idx:student", "@name:Alice", "RETURN", "2", "name", "score", "LIMIT", "0", "10", "DIALECT", "2"
        ), List.of(command));
    }

    @Test
    void buildsJsonSearchCommandWithProjectionAliases() {
        JsonSkuRepository repo = new JsonSkuRepository();

        String[] command = repo.searchCommand(new SearchCondition("@title:Phone").project(List.of("title", "merchant")), 0, 10);

        assertEquals(List.of(
                "FT.SEARCH", "idx:sku-json", "@title:Phone", "RETURN", "6", "$.title", "AS", "title", "$.merchant.name", "AS", "merchant", "LIMIT", "0", "10", "DIALECT", "2"
        ), List.of(command));
    }

    @Test
    void buildsAggregateCommandWithGroupByReducersAndSort() {
        StudentRepository repo = new StudentRepository();
        AggregateRequest request = new AggregateRequest(
                List.of("department"),
                List.of(
                        AggregateReducer.count("total"),
                        AggregateReducer.avg("score", "avg_score")
                ),
                "total",
                false,
                5,
                20
        );

        String[] command = repo.aggregateCommand(new SearchCondition("@status:{ON}"), request);

        assertEquals(List.of(
                "FT.AGGREGATE", "idx:student", "@status:{ON}",
                "LOAD", "2", "@department", "@score",
                "GROUPBY", "1", "@department",
                "REDUCE", "COUNT", "0", "AS", "total",
                "REDUCE", "AVG", "1", "@score", "AS", "avg_score",
                "SORTBY", "2", "@total", "DESC",
                "LIMIT", "5", "20",
                "DIALECT", "2"
        ), List.of(command));
    }

    @Test
    void mapsAggregateResponseRows() {
        StudentRepository repo = new StudentRepository();

        AggregateResult result = repo.mapAggregate(new Object[]{
                2L,
                new Object[]{"department", "Backend", "total", "2", "avg_score", "95.5"},
                new Object[]{"department", "Frontend", "total", 1L, "avg_score", "88.0"}
        });

        assertEquals(2L, result.total());
        assertEquals(2, result.rows().size());
        AggregateRow first = result.rows().get(0);
        assertEquals("Backend", first.getString("department"));
        assertEquals(2L, first.getLong("total"));
        assertEquals(95.5d, first.getDouble("avg_score"));
    }

    @Test
    void mapsAggregateResponseRowsWithoutLeadingTotal() {
        StudentRepository repo = new StudentRepository();

        AggregateResult result = repo.mapAggregate(new Object[]{
                Map.of("department", "Backend", "count", 2L, "score_sum", 170.0d),
                Map.of("department", "Frontend", "count", 1L, "score_sum", 70.0d)
        });

        assertEquals(2L, result.total());
        assertEquals(2, result.rows().size());
        AggregateRow first = result.rows().get(0);
        assertEquals("Backend", first.getString("department"));
        assertEquals(2L, first.getLong("count"));
        assertEquals(170.0d, first.getDouble("score_sum"));
    }

    @Test
    void buildsAggregateCommandWithMinAndMaxReducers() {
        StudentRepository repo = new StudentRepository();
        AggregateRequest request = new AggregateRequest(
                List.of("department"),
                List.of(
                        AggregateReducer.min("score", "min_score"),
                        AggregateReducer.max("score", "max_score")
                ),
                "max_score",
                false,
                0,
                5
        );

        String[] command = repo.aggregateCommand(new SearchCondition("@status:{ON}"), request);

        assertEquals(List.of(
                "FT.AGGREGATE", "idx:student", "@status:{ON}",
                "LOAD", "2", "@department", "@score",
                "GROUPBY", "1", "@department",
                "REDUCE", "MIN", "1", "@score", "AS", "min_score",
                "REDUCE", "MAX", "1", "@score", "AS", "max_score",
                "SORTBY", "2", "@max_score", "DESC",
                "LIMIT", "0", "5",
                "DIALECT", "2"
        ), List.of(command));
    }

    @Test
    void buildsAggregateCommandWithApplyAndFilter() {
        StudentRepository repo = new StudentRepository();
        AggregateRequest request = new AggregateRequest(
                List.of("department"),
                List.of(AggregateReducer.count("count")),
                List.of(new AggregateApply("@count * 10", "weighted_count")),
                List.of("@weighted_count > 10"),
                "weighted_count",
                false,
                0,
                10
        );

        String[] command = repo.aggregateCommand(new SearchCondition("@status:{ON}"), request);

        assertEquals(List.of(
                "FT.AGGREGATE", "idx:student", "@status:{ON}",
                "LOAD", "1", "@department",
                "GROUPBY", "1", "@department",
                "REDUCE", "COUNT", "0", "AS", "count",
                "APPLY", "@count * 10", "AS", "weighted_count",
                "FILTER", "@weighted_count > 10",
                "SORTBY", "2", "@weighted_count", "DESC",
                "LIMIT", "0", "10",
                "DIALECT", "2"
        ), List.of(command));
    }

    @Test
    void aggregateResultCanConvertToFacetResult() {
        AggregateResult aggregate = new AggregateResult(2L, List.of(
                new AggregateRow(Map.of("department", "Backend", "count", 2L)),
                new AggregateRow(Map.of("department", "Frontend", "count", 1L))
        ));

        FacetResult facet = aggregate.toFacet("department");

        assertEquals("department", facet.fieldName());
        assertEquals(2L, facet.total());
        assertEquals(2, facet.buckets().size());
        assertEquals("Backend", facet.buckets().get(0).value());
        assertEquals(2L, facet.buckets().get(0).count());
    }

    @Test
    void aggregateRejectsClusterModeForFirstVersion() {
        ClusterStudentRepository repo = new ClusterStudentRepository(new ClusterAggregateRouting(List.of()));

        ValkeyQueryExecutionException exception = assertThrows(ValkeyQueryExecutionException.class, () -> repo.aggregate(
                new SearchCondition("@status:{ON}"),
                new AggregateRequest(
                        List.of("department"),
                        List.of(AggregateReducer.count("total")),
                        List.of(new AggregateApply("@total * 10", "weighted_total")),
                        List.of(),
                        null,
                        true,
                        0,
                        10
                )
        ));

        assertEquals(ValkeyErrorCode.QUERY_AGGREGATE_FAILED.code(), exception.errorCodeValue());
    }

    @Test
    void aggregateMergesClusterShardResponsesForSafeSubset() {
        ClusterStudentRepository repo = new ClusterStudentRepository(new ClusterAggregateRouting(List.of(
                new Object[]{
                        2L,
                        new Object[]{"department", "Backend", "count", 2L, "__avg_sum_avg_score", 190.0d, "__avg_count_avg_score", 2L, "min_score", 90.0d, "max_score", 100.0d},
                        new Object[]{"department", "Frontend", "count", 1L, "__avg_sum_avg_score", 88.0d, "__avg_count_avg_score", 1L, "min_score", 88.0d, "max_score", 88.0d}
                },
                new Object[]{
                        1L,
                        new Object[]{"department", "Backend", "count", 1L, "__avg_sum_avg_score", 70.0d, "__avg_count_avg_score", 1L, "min_score", 70.0d, "max_score", 70.0d}
                }
        )));

        AggregateResult result = repo.aggregate(
                new SearchCondition("@status:{ON}"),
                new AggregateRequest(
                        List.of("department"),
                        List.of(
                                AggregateReducer.count("count"),
                                AggregateReducer.avg("score", "avg_score"),
                                AggregateReducer.min("score", "min_score"),
                                AggregateReducer.max("score", "max_score")
                        ),
                        "count",
                        false,
                        0,
                        10
                )
        );

        assertEquals(2L, result.total());
        assertEquals(2, result.rows().size());
        AggregateRow backend = result.rows().get(0);
        assertEquals("Backend", backend.getString("department"));
        assertEquals(3L, backend.getLong("count"));
        assertEquals(86.66666666666667d, backend.getDouble("avg_score"));
        assertEquals(70.0d, backend.getDouble("min_score"));
        assertEquals(100.0d, backend.getDouble("max_score"));
    }

    @Test
    void saveAllAggregatesSuccessfulWrites() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);

        BulkWriteResult result = repo.saveAll(List.of(
                BulkSaveItem.of("1", student("Alice")),
                BulkSaveItem.of("2", student("Bob"))
        ), BulkWriteOptions.ordered());

        assertEquals(2, result.submitted());
        assertEquals(2, result.succeeded());
        assertEquals(0, result.failed());
        assertEquals(2, result.items().size());
        assertEquals(1, routing.writeBatchInvocations());
        assertEquals(List.of(
                List.of("HSET", "student:1", "id", "1", "name", "Alice", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON"),
                List.of("HSET", "student:2", "id", "2", "name", "Bob", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON")
        ), routing.commands());
    }

    @Test
    void saveAllStopsOnFirstFailureInOrderedMode() {
        FailingBulkRouting routing = new FailingBulkRouting("student:2");
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);

        BulkWriteResult result = repo.saveAll(List.of(
                BulkSaveItem.of("1", student("Alice")),
                BulkSaveItem.of("2", student("Bob")),
                BulkSaveItem.of("3", student("Carol"))
        ), BulkWriteOptions.ordered());

        assertEquals(3, result.submitted());
        assertEquals(1, result.succeeded());
        assertEquals(1, result.failed());
        assertEquals(2, result.items().size());
        assertEquals(1, routing.writeBatchInvocations());
        assertEquals(List.of(
                List.of("HSET", "student:1", "id", "1", "name", "Alice", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON"),
                List.of("HSET", "student:2", "id", "2", "name", "Bob", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON")
        ), routing.commands());
    }

    @Test
    void saveAllContinuesAfterFailureInUnorderedMode() {
        FailingBulkRouting routing = new FailingBulkRouting("student:2");
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);

        BulkWriteResult result = repo.saveAll(List.of(
                BulkSaveItem.of("1", student("Alice")),
                BulkSaveItem.of("2", student("Bob")),
                BulkSaveItem.of("3", student("Carol"))
        ), BulkWriteOptions.unordered());

        assertEquals(3, result.submitted());
        assertEquals(2, result.succeeded());
        assertEquals(1, result.failed());
        assertEquals(3, result.items().size());
        assertEquals(1, routing.writeBatchInvocations());
        assertEquals(List.of(
                List.of("HSET", "student:1", "id", "1", "name", "Alice", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON"),
                List.of("HSET", "student:2", "id", "2", "name", "Bob", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON"),
                List.of("HSET", "student:3", "id", "3", "name", "Carol", "age", "18", "score", "95.5", "class_name", "Class A", "department", "Backend", "status", "ON")
        ), routing.commands());
    }

    @Test
    void deleteAllAggregatesDeleteResults() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);

        BulkWriteResult result = repo.deleteAll(List.of("1", "2"), BulkWriteOptions.ordered());

        assertEquals(2, result.submitted());
        assertEquals(2, result.succeeded());
        assertEquals(0, result.failed());
        assertEquals(1, routing.writeBatchInvocations());
        assertEquals(List.of(
                List.of("DEL", "student:1"),
                List.of("DEL", "student:2")
        ), routing.commands());
    }

    @Test
    void deleteAllContinuesAfterFailureInUnorderedMode() {
        FailingBulkRouting routing = new FailingBulkRouting("student:2");
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);

        BulkWriteResult result = repo.deleteAll(List.of("1", "2", "3"), BulkWriteOptions.unordered());

        assertEquals(3, result.submitted());
        assertEquals(2, result.succeeded());
        assertEquals(1, result.failed());
        assertEquals(1, routing.writeBatchInvocations());
        assertEquals(List.of(
                List.of("DEL", "student:1"),
                List.of("DEL", "student:2"),
                List.of("DEL", "student:3")
        ), routing.commands());
    }

    @Test
    void updateChainUsesHashPartialUpdateCommand() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        StudentQuery q = new StudentQuery();

        long updated = repo.updateChain()
                .set(q.score, 98.5d)
                .set(q.status, "OFF")
                .whereId(1L)
                .execute();

        assertEquals(1L, updated);
        assertEquals(List.of(
                List.of("EXISTS", "student:1"),
                List.of("HSET", "student:1", "score", "98.5", "status", "OFF")
        ), routing.commands());
    }

    @Test
    void updateChainSupportsWhereAndSetIfDsl() {
        RecordingRouting routing = new RecordingRouting(true, 1L);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        StudentQuery q = new StudentQuery();

        long updated = repo.updateChain()
                .where(q.id.eq("1"))
                .and(q.status.eq("ON"))
                .set(q.score, 98.5d)
                .setIf(true, q.status, "OFF")
                .execute();

        assertEquals(1L, updated);
        assertEquals(1, routing.commands().size());
        List<String> command = routing.commands().get(0);
        assertEquals("EVALSHA", command.get(0));
        assertTrue(command.contains("id"));
        assertTrue(command.contains("1"));
        assertTrue(command.contains("status"));
        assertTrue(command.contains("ON"));
        assertTrue(command.contains("score"));
        assertTrue(command.contains("98.5"));
        assertTrue(command.contains("OFF"));
    }

    @Test
    void updateChainUsesJsonSetPerFieldAndPreservesAliasPath() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        long updated = repo.updateChain()
                .set(q.price, 4099)
                .set(q.merchant.name, "Updated Merchant")
                .whereId("sku-1")
                .execute();

        assertEquals(1L, updated);
        assertEquals(List.of(
                List.of("EXISTS", "sku:sku-1"),
                List.of("JSON.SET", "sku:sku-1", "$.price", "4099"),
                List.of("JSON.SET", "sku:sku-1", "$.merchant.name", "\"Updated Merchant\"")
        ), routing.commands());
    }

    @Test
    void updateChainSupportsOrAndOrIfDsl() {
        RecordingRouting routing = new RecordingRouting(true, 1L);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        long updated = repo.updateChain()
                .whereId("sku-9")
                .where(q.id.eq("sku-9"))
                .and(q.merchant.name.eq("Before Merchant").or(q.merchant.name.eq("Fallback Merchant")))
                .orIf(true, () -> q.price.eq(1999))
                .set(q.merchant.name, "After Merchant")
                .execute();

        assertEquals(1L, updated);
        assertEquals(1, routing.commands().size());
        List<String> command = routing.commands().get(0);
        assertEquals("EVALSHA", command.get(0));
        assertTrue(command.contains(".merchant.name"));
        assertTrue(command.contains("\"Before Merchant\""));
        assertTrue(command.contains("\"Fallback Merchant\""));
        assertTrue(command.contains("1999"));
    }

    @Test
    void updateChainRejectsIdInferenceWhenPredicateContainsOr() {
        RecordingRouting routing = new RecordingRouting(true, 1L);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> repo.updateChain()
                .where(q.id.eq("sku-9"))
                .or(q.price.eq(1999))
                .set(q.merchant.name, "After Merchant")
                .execute());

        assertTrue(exception.getMessage().contains("containing OR"));
        assertTrue(routing.commands().isEmpty());
    }

    @Test
    void updateChainNormalizesJsonCollectionTags() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        long updated = repo.updateChain()
                .set(q.tags, List.of("HOT", "NEW"))
                .whereId("sku-2")
                .execute();

        assertEquals(1L, updated);
        assertEquals(List.of(
                List.of("EXISTS", "sku:sku-2"),
                List.of("JSON.SET", "sku:sku-2", "$.tags", "\"HOT,NEW\"")
        ), routing.commands());
    }

    @Test
    void updateChainReturnsZeroWhenKeyDoesNotExist() {
        RecordingRouting routing = new RecordingRouting(false);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        long updated = repo.updateChain()
                .set(q.price, 1999)
                .whereId("missing")
                .execute();

        assertEquals(0L, updated);
        assertEquals(List.of(
                List.of("EXISTS", "sku:missing")
        ), routing.commands());
    }

    @Test
    void updateChainUsesConditionalHashScriptWhenExpectationsPresent() {
        RecordingRouting routing = new RecordingRouting(true, 1L);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        StudentQuery q = new StudentQuery();

        long updated = repo.updateChain()
                .where(q.id.eq("1"))
                .and(q.status.eq("ON"))
                .set(q.score, 99.0d)
                .execute();

        assertEquals(1L, updated);
        assertEquals(1, routing.commands().size());
        List<String> command = routing.commands().get(0);
        assertEquals("EVALSHA", command.get(0));
        assertEquals("1", command.get(2));
        assertEquals("student:1", command.get(3));
        assertTrue(command.contains("status"));
        assertTrue(command.contains("ON"));
        assertTrue(command.contains("score"));
        assertTrue(command.contains("99.0"));
    }

    @Test
    void updateChainUsesConditionalJsonScriptWhenExpectationsPresent() {
        RecordingRouting routing = new RecordingRouting(true, 1L);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        long updated = repo.updateChain()
                .where(q.id.eq("sku-3"))
                .and(q.merchant.name.eq("Before Merchant"))
                .set(q.merchant.name, "After Merchant")
                .execute();

        assertEquals(1L, updated);
        assertEquals(1, routing.commands().size());
        List<String> command = routing.commands().get(0);
        assertEquals("EVALSHA", command.get(0));
        assertEquals("sku:sku-3", command.get(3));
        assertTrue(command.contains(".merchant.name"));
        assertTrue(command.contains("$.merchant.name"));
        assertTrue(command.contains("\"Before Merchant\""));
        assertTrue(command.contains("\"After Merchant\""));
    }

    @Test
    void updateChainRecordsUpdatedAndNotFoundMetrics() {
        RecordingUpdateMetricsRecorder metrics = new RecordingUpdateMetricsRecorder();
        StudentQuery q = new StudentQuery();

        RecordingRouting successRouting = new RecordingRouting(true);
        RoutedStudentRepository successRepo = new RoutedStudentRepository(successRouting, metrics);
        assertEquals(1L, successRepo.updateChain()
                .whereId(1L)
                .set(q.score, 100.0d)
                .execute());

        RecordingRouting missingRouting = new RecordingRouting(false);
        RoutedStudentRepository missingRepo = new RoutedStudentRepository(missingRouting, metrics);
        assertEquals(0L, missingRepo.updateChain()
                .whereId(2L)
                .set(q.score, 99.0d)
                .execute());

        assertEquals(List.of("idx:student|updated|set", "idx:student|not_found|set"), metrics.outcomes());
    }

    @Test
    void updateChainRecordsFailureMetric() {
        RecordingUpdateMetricsRecorder metrics = new RecordingUpdateMetricsRecorder();
        FailingRouting routing = new FailingRouting();
        RoutedStudentRepository repo = new RoutedStudentRepository(routing, metrics);
        StudentQuery q = new StudentQuery();

        ValkeyQueryExecutionException exception = assertThrows(ValkeyQueryExecutionException.class, () -> repo.updateChain()
                .whereId(1L)
                .set(q.score, 101.0d)
                .execute());

        assertEquals(ValkeyErrorCode.QUERY_UPDATE_FAILED, exception.errorCode());
        assertEquals(List.of("idx:student|QUERY_003|QUERY|set"), metrics.failures());
    }

    @Test
    void expectVersionRejectsNonVersionField() {
        RoutedStudentRepository repo = new RoutedStudentRepository(new RecordingRouting(true));
        StudentQuery q = new StudentQuery();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> repo.updateChain()
                .whereId(1L)
                .expectVersion(q.score, 3));

        assertTrue(exception.getMessage().contains("version-like"));
    }

    @Test
    void advanceVersionSetsNextVersionValue() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        ValkeyFieldReference versionField = () -> "version";

        long updated = repo.updateChain()
                .whereId(1L)
                .advanceVersion(versionField, 3)
                .execute();

        assertEquals(1L, updated);
        assertEquals(1, routing.commands().size());
        List<String> command = routing.commands().get(0);
        assertEquals("EVALSHA", command.get(0));
        assertTrue(command.contains("student:1"));
        assertTrue(command.contains("version"));
        assertTrue(command.contains("3"));
        assertTrue(command.contains("4"));
    }

    @Test
    void updateChainReturnsZeroWhenExpectationDoesNotMatch() {
        RecordingRouting routing = new RecordingRouting(true, 0L);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        StudentQuery q = new StudentQuery();

        long updated = repo.updateChain()
                .where(q.id.eq("1"))
                .and(q.status.eq("ON"))
                .set(q.status, "OFF")
                .execute();

        assertEquals(0L, updated);
        assertEquals(1, routing.commands().size());
        assertEquals("EVALSHA", routing.commands().get(0).get(0));
    }

    @Test
    void updateChainUsesNativeHashIncrementCommand() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        StudentQuery q = new StudentQuery();

        long updated = repo.updateChain()
                .whereId(1L)
                .increment(q.age, 2)
                .execute();

        assertEquals(1L, updated);
        assertEquals(List.of(
                List.of("EXISTS", "student:1"),
                List.of("HINCRBY", "student:1", "age", "2")
        ), routing.commands());
    }

    @Test
    void updateChainUsesNativeJsonDecrementCommand() {
        RecordingRouting routing = new RecordingRouting(true);
        RoutedJsonSkuRepository repo = new RoutedJsonSkuRepository(routing);
        SkuQuery q = new SkuQuery();

        long updated = repo.updateChain()
                .whereId("sku-4")
                .decrement(q.price, 100)
                .execute();

        assertEquals(1L, updated);
        assertEquals(List.of(
                List.of("EXISTS", "sku:sku-4"),
                List.of("JSON.NUMINCRBY", "sku:sku-4", "$.price", "-100")
        ), routing.commands());
    }

    @Test
    void updateChainUsesScriptedMixedHashUpdateForSetAndIncrement() {
        RecordingRouting routing = new RecordingRouting(true, 1L);
        RoutedStudentRepository repo = new RoutedStudentRepository(routing);
        StudentQuery q = new StudentQuery();

        long updated = repo.updateChain()
                .where(q.id.eq("1"))
                .and(q.status.eq("ON"))
                .set(q.status, "OFF")
                .increment(q.age, 5)
                .execute();

        assertEquals(1L, updated);
        assertEquals(1, routing.commands().size());
        List<String> command = routing.commands().get(0);
        assertEquals("EVALSHA", command.get(0));
        assertTrue(command.contains("SET"));
        assertTrue(command.contains("INCREMENT"));
        assertTrue(command.contains("age"));
        assertTrue(command.contains("5"));
    }

    @Test
    void repositoryWithoutClientThrowsConfigurationException() {
        StudentRepository repo = new StudentRepository();

        ValkeyConfigurationException exception = assertThrows(ValkeyConfigurationException.class, repo::requireClient);

        assertTrue(exception.getMessage().contains("Valkey 客户端未注入"));
        assertEquals(ValkeyErrorCode.CONFIGURATION_ERROR, exception.errorCode());
    }

    @Test
    void invalidStoredJsonThrowsResultMappingException() {
        JsonSkuRepository repo = new JsonSkuRepository();

        ValkeyResultMappingException exception = assertThrows(
                ValkeyResultMappingException.class,
                () -> repo.convert(Map.of("$", "{bad-json}"))
        );

        assertTrue(exception.getMessage().contains("结果映射失败"));
        assertEquals(ValkeyErrorCode.RESULT_MAPPING_ERROR, exception.errorCode());
    }

    @Test
    void clusterSearchRejectsOversizedMergeWindow() {
        ClusterStudentRepository repo = new ClusterStudentRepository(new ClusterRecordingRouting());

        ValkeyQueryExecutionException exception = assertThrows(
                ValkeyQueryExecutionException.class,
                () -> repo.page(new SearchCondition("@name:Alice"), 1900, 200)
        );

        assertTrue(exception.getMessage().contains("Cluster merge search window exceeds supported limit"));
        assertEquals(ValkeyErrorCode.QUERY_SEARCH_FAILED, exception.errorCode());
    }

    @Test
    void clusterSearchMergesSortedWindowWithoutMaterializingAllShardRecords() {
        ClusterStudentRepository repo = new ClusterStudentRepository(new ClusterSearchRouting(List.of(
                new Object[]{2L, studentShard(
                        studentDocument("student:1", "Alice", "30.0"),
                        studentDocument("student:2", "Bob", "10.0")
                )},
                new Object[]{2L, studentShard(
                        studentDocument("student:3", "Carol", "40.0"),
                        studentDocument("student:4", "Dave", "20.0")
                )}
        )));

        Page<Student> page = repo.page(new SearchCondition("*").sortBy("score", true), 1, 2);

        assertEquals(4L, page.total());
        assertEquals(2, page.records().size());
        assertEquals(20.0d, page.records().get(0).getScore());
        assertEquals(30.0d, page.records().get(1).getScore());
    }

    static class StudentRepository extends BaseValkeyRepository<Student> {

        StudentRepository() {
            super(StudentQuery.METADATA, (GlideClient) null, Student.class, new ObjectMapper().findAndRegisterModules());
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

        String[] aggregateCommand(SearchCondition condition, AggregateRequest request) {
            return buildAggregateCommand(condition, request);
        }

        String[] createCommand() {
            return buildCreateCommand();
        }

        AggregateResult mapAggregate(Object[] response) {
            return mapAggregateResponse(response);
        }

        void requireClient() {
            requireRouting();
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
            ), (GlideClient) null, Sku.class, new ObjectMapper().findAndRegisterModules());
        }

        SearchResult<Sku> map(Object[] response) {
            return mapSearchResponse(response);
        }

        String[] createCommand() {
            return buildCreateCommand();
        }

        String[] searchCommand(SearchCondition condition, int offset, int limit) {
            return buildSearchCommand(condition, offset, limit);
        }

        Sku convert(Map<String, ?> fields) {
            return convertStoredFields(fields);
        }
    }

    static class RoutedStudentRepository extends BaseValkeyRepository<Student> {

        RoutedStudentRepository(GlideClient writeClient, GlideClient readClient) {
            super(StudentQuery.METADATA,
                    new DefaultValkeyClientRouting(writeClient, readClient, false),
                    Student.class,
                    new ObjectMapper().findAndRegisterModules());
        }

        RoutedStudentRepository(RecordingRouting routing) {
            super(StudentQuery.METADATA,
                    routing,
                    Student.class,
                    new ObjectMapper().findAndRegisterModules());
        }

        RoutedStudentRepository(RecordingRouting routing, ValkeyUpdateMetricsRecorder updateMetricsRecorder) {
            super(StudentQuery.METADATA,
                    routing,
                    Student.class,
                    new ObjectMapper().findAndRegisterModules(),
                    null,
                    updateMetricsRecorder);
        }

        void executeWrite() throws Exception {
            executeWriteCommand(new String[]{"PING"});
        }

        void executeRead() throws Exception {
            executeReadCommand(new String[]{"FT._LIST"});
        }

        void executeIndex() throws Exception {
            executeIndexCommand(new String[]{"FT.INFO", "idx:student"});
        }
    }

    static class RoutedJsonSkuRepository extends BaseValkeyRepository<Sku> {

        RoutedJsonSkuRepository(RecordingRouting routing) {
            super(SkuQuery.METADATA,
                    routing,
                    Sku.class,
                    new ObjectMapper().findAndRegisterModules());
        }
    }

    static class ClusterStudentRepository extends BaseValkeyRepository<Student> {

        ClusterStudentRepository(com.momao.valkey.adapter.ValkeyClientRouting routing) {
            super(StudentQuery.METADATA,
                    routing,
                    Student.class,
                    new ObjectMapper().findAndRegisterModules());
        }
    }

    static class RecordingRouting implements com.momao.valkey.adapter.ValkeyClientRouting {

        private final boolean exists;

        private final long conditionalResult;

        private final List<List<String>> commands = new ArrayList<>();

        private int writeBatchInvocations;

        RecordingRouting(boolean exists) {
            this(exists, 1L);
        }

        RecordingRouting(boolean exists, long conditionalResult) {
            this.exists = exists;
            this.conditionalResult = conditionalResult;
        }

        @Override
        public Object executeWrite(String[] command) {
            commands.add(List.copyOf(java.util.Arrays.asList(command.clone())));
            if ("EVALSHA".equals(command[0])) {
                return conditionalResult;
            }
            if ("SCRIPT".equals(command[0]) && "LOAD".equals(command[1])) {
                return "stub-sha";
            }
            if ("DEL".equals(command[0])) {
                return 1L;
            }
            return "OK";
        }

        @Override
        public Object executeRead(String[] command) {
            commands.add(List.copyOf(java.util.Arrays.asList(command.clone())));
            if ("EXISTS".equals(command[0])) {
                return exists ? 1L : 0L;
            }
            return null;
        }

        @Override
        public List<com.momao.valkey.adapter.BatchCommandResult> executeWriteBatch(List<String[]> commands, com.momao.valkey.core.BulkMode mode) {
            writeBatchInvocations++;
            return com.momao.valkey.adapter.ValkeyClientRouting.super.executeWriteBatch(commands, mode);
        }

        List<List<String>> commands() {
            return commands;
        }

        int writeBatchInvocations() {
            return writeBatchInvocations;
        }
    }

    static class FailingRouting extends RecordingRouting {

        FailingRouting() {
            super(true);
        }

        @Override
        public Object executeWrite(String[] command) {
            throw new RuntimeException("update failed");
        }
    }

    static class FailingBulkRouting extends RecordingRouting {

        private final String failingKey;

        FailingBulkRouting(String failingKey) {
            super(true);
            this.failingKey = failingKey;
        }

        @Override
        public Object executeWrite(String[] command) {
            commands().add(List.copyOf(java.util.Arrays.asList(command.clone())));
            if (java.util.Arrays.asList(command).contains(failingKey)) {
                throw new RuntimeException("bulk failed");
            }
            return "DEL".equals(command[0]) ? 1L : "OK";
        }
    }

    static class ClusterRecordingRouting extends RecordingRouting {

        ClusterRecordingRouting() {
            super(true);
        }

        @Override
        public boolean isClusterMode() {
            return true;
        }
    }

    static class ClusterSearchRouting extends ClusterRecordingRouting {

        private final List<Object> shardResponses;

        ClusterSearchRouting(List<Object> shardResponses) {
            this.shardResponses = shardResponses;
        }

        @Override
        public List<Object> executeReadAll(String[] command) {
            return shardResponses;
        }
    }

    static class ClusterAggregateRouting extends ClusterRecordingRouting {

        private final List<Object> shardResponses;

        ClusterAggregateRouting(List<Object> shardResponses) {
            this.shardResponses = shardResponses;
        }

        @Override
        public List<Object> executeReadAll(String[] command) {
            return shardResponses;
        }
    }

    private static Map<String, Map<String, String>> studentShard(Map.Entry<String, Map<String, String>>... entries) {
        Map<String, Map<String, String>> shard = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : entries) {
            shard.put(entry.getKey(), entry.getValue());
        }
        return shard;
    }

    private static Map.Entry<String, Map<String, String>> studentDocument(String key, String name, String score) {
        return Map.entry(key, Map.of(
                "name", name,
                "age", "18",
                "score", score,
                "class_name", "Class A",
                "department", "Backend",
                "status", "ON"
        ));
    }

    private static Student student(String name) {
        Student student = new Student();
        student.setId(Long.parseLong("Alice".equals(name) ? "1" : "Bob".equals(name) ? "2" : "3"));
        student.setName(name);
        student.setAge(18);
        student.setScore(95.5d);
        student.setClassName("Class A");
        student.setDepartment("Backend");
        student.setStatus("ON");
        return student;
    }

    static class RecordingUpdateMetricsRecorder implements ValkeyUpdateMetricsRecorder {

        private final List<String> outcomes = new ArrayList<>();
        private final List<String> failures = new ArrayList<>();

        @Override
        public void recordPartialUpdate(String indexName, String outcome, String updateKind) {
            outcomes.add(indexName + "|" + outcome + "|" + updateKind);
        }

        @Override
        public void recordPartialUpdateFailure(String indexName, String errorCode, String errorCategory, String updateKind) {
            failures.add(indexName + "|" + errorCode + "|" + errorCategory + "|" + updateKind);
        }

        List<String> outcomes() {
            return outcomes;
        }

        List<String> failures() {
            return failures;
        }
    }
}
