package com.momao.valkey.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValkeyQueryChainTests {

    @Test
    void selectWritesProjectionIntoFinalCondition() {
        CapturingRepository repository = new CapturingRepository();
        ValkeyQueryChain<Object> chain = new ValkeyQueryChain<>(repository);

        chain.where(new SearchCondition("@name:Alice"))
                .select("id", "title", "title")
                .list();

        assertEquals(List.of("id", "title"), repository.captured.selectedFields());
    }

    @Test
    void aggregateChainBuildsGroupingReducersAndLimit() {
        CapturingRepository repository = new CapturingRepository();
        ValkeyAggregateChain<Object> chain = repository.aggregateChain();

        chain.where(new SearchCondition("@status:{ON}"))
                .groupBy(new TagFieldBuilder("department"))
                .count("total")
                .avg(new NumericFieldBuilder("score"), "avg_score")
                .sortByDesc("total")
                .limit(5, 20)
                .result();

        assertEquals("@status:{ON}", repository.aggregateCondition.build());
        assertEquals(List.of("department"), repository.aggregateRequest.groupByFields());
        assertEquals(2, repository.aggregateRequest.reducers().size());
        assertEquals("total", repository.aggregateRequest.reducers().get(0).alias());
        assertEquals("avg_score", repository.aggregateRequest.reducers().get(1).alias());
        assertEquals("total", repository.aggregateRequest.sortField());
        assertEquals(false, repository.aggregateRequest.sortAscending());
        assertEquals(5, repository.aggregateRequest.offset());
        assertEquals(20, repository.aggregateRequest.limit());
    }

    @Test
    void aggregateChainSupportsMinAndMaxReducers() {
        CapturingRepository repository = new CapturingRepository();

        repository.aggregateChain()
                .groupBy(new TagFieldBuilder("department"))
                .min(new NumericFieldBuilder("score"), "min_score")
                .max(new NumericFieldBuilder("score"), "max_score")
                .result();

        assertEquals(2, repository.aggregateRequest.reducers().size());
        assertEquals(AggregateReducerKind.MIN, repository.aggregateRequest.reducers().get(0).kind());
        assertEquals("min_score", repository.aggregateRequest.reducers().get(0).alias());
        assertEquals(AggregateReducerKind.MAX, repository.aggregateRequest.reducers().get(1).kind());
        assertEquals("max_score", repository.aggregateRequest.reducers().get(1).alias());
    }

    @Test
    void facetChainBuildsSingleFieldCountAggregation() {
        CapturingRepository repository = new CapturingRepository();

        repository.facetChain()
                .where(new SearchCondition("@status:{ON}"))
                .on(new TagFieldBuilder("department"))
                .size(5)
                .result();

        assertEquals("@status:{ON}", repository.aggregateCondition.build());
        assertEquals(List.of("department"), repository.aggregateRequest.groupByFields());
        assertEquals(1, repository.aggregateRequest.reducers().size());
        assertEquals(AggregateReducerKind.COUNT, repository.aggregateRequest.reducers().get(0).kind());
        assertEquals("count", repository.aggregateRequest.reducers().get(0).alias());
        assertEquals("count", repository.aggregateRequest.sortField());
        assertEquals(false, repository.aggregateRequest.sortAscending());
        assertEquals(5, repository.aggregateRequest.limit());
    }

    @Test
    void facetChainSupportsSortByValueAndCustomCountAlias() {
        CapturingRepository repository = new CapturingRepository();

        repository.facetChain()
                .on(new TagFieldBuilder("department"))
                .countAlias("total")
                .sortByValueAsc()
                .result();

        assertEquals("department", repository.aggregateRequest.sortField());
        assertEquals(true, repository.aggregateRequest.sortAscending());
        assertEquals("total", repository.aggregateRequest.reducers().get(0).alias());
    }

    @Test
    void facetChainSupportsMultiFieldResults() {
        CapturingRepository repository = new CapturingRepository();

        FacetResults results = repository.facetChain()
                .where(new SearchCondition("@status:{ON}"))
                .on(new TagFieldBuilder("department"), new TagFieldBuilder("city"))
                .size(5)
                .results();

        assertEquals(2, repository.aggregateRequests.size());
        assertEquals(List.of("department"), repository.aggregateRequests.get(0).groupByFields());
        assertEquals(List.of("city"), repository.aggregateRequests.get(1).groupByFields());
        assertEquals("department", results.facets().get(0).fieldName());
        assertEquals("city", results.facets().get(1).fieldName());
    }

    @Test
    void facetChainSupportsClientSideMinCountFiltering() {
        CapturingRepository repository = new CapturingRepository();
        repository.aggregateResponses.add(new AggregateResult(3L, List.of(
                new AggregateRow(java.util.Map.of("department", "Backend", "count", 3L)),
                new AggregateRow(java.util.Map.of("department", "Frontend", "count", 1L)),
                new AggregateRow(java.util.Map.of("department", "QA", "count", 2L))
        )));

        FacetResult facet = repository.facetChain()
                .on(new TagFieldBuilder("department"))
                .minCount(2)
                .result();

        assertEquals("department", facet.fieldName());
        assertEquals(2L, facet.total());
        assertEquals(2, facet.buckets().size());
        assertEquals("Backend", facet.buckets().get(0).value());
        assertEquals(3L, facet.buckets().get(0).count());
        assertEquals("QA", facet.buckets().get(1).value());
        assertEquals(2L, facet.buckets().get(1).count());
    }

    @Test
    void facetChainSupportsIncludedValuesFiltering() {
        CapturingRepository repository = new CapturingRepository();
        repository.aggregateResponses.add(new AggregateResult(3L, List.of(
                new AggregateRow(java.util.Map.of("department", "Backend", "count", 3L)),
                new AggregateRow(java.util.Map.of("department", "Frontend", "count", 1L)),
                new AggregateRow(java.util.Map.of("department", "QA", "count", 2L))
        )));

        FacetResult facet = repository.facetChain()
                .on(new TagFieldBuilder("department"))
                .includeValues("QA", "Backend")
                .result();

        assertEquals(2L, facet.total());
        assertEquals(2, facet.buckets().size());
        assertEquals("Backend", facet.buckets().get(0).value());
        assertEquals("QA", facet.buckets().get(1).value());
    }

    @Test
    void facetChainRejectsSingleResultForMultiFieldFacet() {
        CapturingRepository repository = new CapturingRepository();

        ValkeyFacetChain<Object> chain = repository.facetChain()
                .on(new TagFieldBuilder("department"), new TagFieldBuilder("city"));

        assertThrows(IllegalStateException.class, chain::result);
    }

    @Test
    void distinctRowsBuildsMultiFieldGroupByAggregation() {
        CapturingRepository repository = new CapturingRepository();

        repository.queryChain()
                .where(new SearchCondition("@status:{ON}"))
                .distinctRows(5, 20, new TagFieldBuilder("department"), new TagFieldBuilder("city"));

        assertEquals("@status:{ON}", repository.aggregateCondition.build());
        assertEquals(List.of("department", "city"), repository.aggregateRequest.groupByFields());
        assertEquals(0, repository.aggregateRequest.reducers().size());
        assertEquals(5, repository.aggregateRequest.offset());
        assertEquals(20, repository.aggregateRequest.limit());
    }

    @Test
    void distinctRowsSupportsLambdaMappingWithoutReflection() {
        CapturingRepository repository = new CapturingRepository();

        List<String> pairs = repository.queryChain()
                .distinctRows(row -> row.getString("department") + ":" + row.getString("city"),
                        new TagFieldBuilder("department"),
                        new TagFieldBuilder("city"));

        assertEquals(List.of("department:city"), pairs);
    }

    @Test
    void distinctRowsSupportsTypeMapping() {
        CapturingRepository repository = new CapturingRepository();

        List<DistinctPairRecord> rows = repository.queryChain()
                .distinctRows(DistinctPairRecord.class,
                        new TagFieldBuilder("department"),
                        new TagFieldBuilder("city"));

        assertEquals(1, rows.size());
        assertEquals("department", rows.get(0).department());
        assertEquals("city", rows.get(0).city());
    }

    @Test
    void countDistinctBuildsSortedSingleFieldAggregation() {
        CapturingRepository repository = new CapturingRepository();

        long total = repository.queryChain()
                .where(new SearchCondition("@status:{ON}"))
                .countDistinct(new TagFieldBuilder("department"));

        assertEquals(1L, total);
        assertEquals("@status:{ON}", repository.aggregateCondition.build());
        assertEquals(List.of("department"), repository.aggregateRequest.groupByFields());
        assertEquals(0, repository.aggregateRequest.reducers().size());
        assertEquals(0, repository.aggregateRequest.offset());
        assertEquals(1_000, repository.aggregateRequest.limit());
        assertEquals("department", repository.aggregateRequest.sortField());
        assertEquals(true, repository.aggregateRequest.sortAscending());
    }

    @Test
    void aggregateChainSupportsApplyAndFilter() {
        CapturingRepository repository = new CapturingRepository();

        repository.aggregateChain()
                .groupBy(new TagFieldBuilder("department"))
                .count("count")
                .apply("@count * 10", "weighted_count")
                .filter("@weighted_count > 10")
                .result();

        assertEquals(1, repository.aggregateRequest.applies().size());
        assertEquals("@count * 10", repository.aggregateRequest.applies().get(0).expression());
        assertEquals("weighted_count", repository.aggregateRequest.applies().get(0).alias());
        assertEquals(List.of("@weighted_count > 10"), repository.aggregateRequest.filters());
    }

    @Test
    void aggregateChainSupportsTypedApplyAndFilterDsl() {
        CapturingRepository repository = new CapturingRepository();

        repository.aggregateChain()
                .groupBy(new TagFieldBuilder("department"))
                .count("count")
                .apply(AggregateExpressions.field("count").multiply(10), "weighted_count")
                .filter(
                        AggregateExpressions.field("weighted_count").gt(10)
                                .and(AggregateExpressions.field("weighted_count").lt(100))
                )
                .result();

        assertEquals("@count * 10", repository.aggregateRequest.applies().get(0).expression());
        assertEquals(List.of("(@weighted_count > 10) && (@weighted_count < 100)"), repository.aggregateRequest.filters());
    }

    @Test
    void aggregateResultMapsRowsToRecord() {
        AggregateResult result = new AggregateResult(1L, List.of(
                new AggregateRow(java.util.Map.of("department", "Backend", "count", 2L, "avg_score", 95.5d))
        ));

        List<DepartmentStatsRecord> rows = result.mapRows(DepartmentStatsRecord.class);

        assertEquals(1, rows.size());
        assertEquals("Backend", rows.get(0).department());
        assertEquals(2L, rows.get(0).count());
        assertEquals(95.5d, rows.get(0).avg_score());
    }

    @Test
    void aggregateResultMapsRowsWithLambdaWithoutReflection() {
        AggregateResult result = new AggregateResult(1L, List.of(
                new AggregateRow(java.util.Map.of("department", "Backend", "count", 2L))
        ));

        List<DepartmentStatsBean> rows = result.mapRows(row -> {
            DepartmentStatsBean bean = new DepartmentStatsBean();
            bean.department = row.getString("department");
            bean.count = row.getLong("count");
            return bean;
        });

        assertEquals(1, rows.size());
        assertEquals("Backend", rows.get(0).department);
        assertEquals(2L, rows.get(0).count);
    }

    @Test
    void aggregateResultMapsRowsToBean() {
        AggregateResult result = new AggregateResult(1L, List.of(
                new AggregateRow(java.util.Map.of("department", "Frontend", "count", "3"))
        ));

        List<DepartmentStatsBean> rows = result.mapRows(DepartmentStatsBean.class);

        assertEquals(1, rows.size());
        assertEquals("Frontend", rows.get(0).department);
        assertEquals(3L, rows.get(0).count);
    }

    private static final class CapturingRepository implements ValkeyRepository<Object> {

        private SearchCondition captured;
        private SearchCondition aggregateCondition;
        private AggregateRequest aggregateRequest;
        private final java.util.List<AggregateRequest> aggregateRequests = new java.util.ArrayList<>();
        private final java.util.List<AggregateResult> aggregateResponses = new java.util.ArrayList<>();

        @Override
        public String checkAndCreateIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void save(String id, Object entity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long updateById(Object id, List<UpdateOperation> operations, SearchPredicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchResult<Object> search(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Object> list(SearchCondition condition) {
            this.captured = condition;
            return List.of();
        }

        @Override
        public Page<Object> page(SearchCondition condition, int offset, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object one(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregateResult aggregate(SearchCondition condition, AggregateRequest request) {
            this.aggregateCondition = condition;
            this.aggregateRequest = request;
            this.aggregateRequests.add(request);
            if (!aggregateResponses.isEmpty()) {
                return aggregateResponses.remove(0);
            }
            java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
            if (request.groupByFields().isEmpty()) {
                row.put("unknown", "unknown");
            } else {
                for (String fieldName : request.groupByFields()) {
                    row.put(fieldName, fieldName);
                }
            }
            row.put("count", 1L);
            return new AggregateResult(1L, List.of(new AggregateRow(row)));
        }

        @Override
        public String getIndexName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getPrefix() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getPrefixes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<com.momao.valkey.core.metadata.SchemaField> getFields() {
            throw new UnsupportedOperationException();
        }
    }

    private record DepartmentStatsRecord(String department, long count, double avg_score) {
    }

    private record DistinctPairRecord(String department, String city) {
    }

    public static final class DepartmentStatsBean {
        public String department;
        public long count;
    }
}
