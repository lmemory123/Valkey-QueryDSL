package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.core.AggregateResult;
import com.momao.valkey.core.FacetResult;
import com.momao.valkey.core.FacetResults;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_CLUSTER_INTEGRATION", matches = "true")
@TestPropertySource(properties = {
        "valkey.query.enabled=true",
        "valkey.query.mode=cluster"
})
class ClusterAggregateIntegrationTests {

    @Autowired
    private ValkeyClientRouting clientRouting;

    @Autowired
    private ObjectMapper objectMapper;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("valkey.query.cluster.nodes[0].host", () -> getenv("VALKEY_CLUSTER_HOST", "localhost"));
        registry.add("valkey.query.cluster.nodes[0].port", () -> getenvInt("VALKEY_CLUSTER_PORT", 8000));
    }

    @Test
    void countBasedAggregateWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cagg" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");
        repository.saveStudent(token + "-4", "Dave-" + token, 21, 60.0d, "Class-" + token + "-2", departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 4L);

        AggregateResult result = repository.aggregateChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .groupBy(q.department)
                .count("count")
                .sortByDesc("count")
                .limit(0, 10)
                .result();

        assertEquals(2L, result.total());
        assertEquals(departmentA, result.rows().get(0).getString("department"));
        assertEquals(2L, result.rows().get(0).getLong("count"));
        assertEquals(departmentB, result.rows().get(1).getString("department"));
        assertEquals(1L, result.rows().get(1).getLong("count"));
    }

    @Test
    void reducerAggregateWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "caggr" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        AggregateResult result = repository.aggregateChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .groupBy(q.department)
                .count("count")
                .sum(q.score, "score_sum")
                .avg(q.score, "score_avg")
                .min(q.age, "age_min")
                .max(q.age, "age_max")
                .sortByDesc("count")
                .limit(0, 10)
                .result();

        assertEquals(2L, result.total());
        assertEquals(departmentA, result.rows().get(0).getString("department"));
        assertEquals(2L, result.rows().get(0).getLong("count"));
        assertEquals(170.0d, result.rows().get(0).getDouble("score_sum"));
        assertEquals(85.0d, result.rows().get(0).getDouble("score_avg"));
        assertEquals(18L, result.rows().get(0).getLong("age_min"));
        assertEquals(19L, result.rows().get(0).getLong("age_max"));
        assertEquals(departmentB, result.rows().get(1).getString("department"));
        assertEquals(1L, result.rows().get(1).getLong("count"));
        assertEquals(70.0d, result.rows().get(1).getDouble("score_sum"));
        assertEquals(70.0d, result.rows().get(1).getDouble("score_avg"));
        assertEquals(20L, result.rows().get(1).getLong("age_min"));
        assertEquals(20L, result.rows().get(1).getLong("age_max"));

        List<DepartmentStats> mapped = result.mapRows(DepartmentStats.class);
        DepartmentStats departmentAStats = mapped.stream()
                .filter(row -> departmentA.equals(row.department()))
                .findFirst()
                .orElseThrow();
        assertEquals(2L, departmentAStats.count());
        assertEquals(170.0d, departmentAStats.score_sum());
        assertEquals(85.0d, departmentAStats.score_avg());
    }

    @Test
    void facetChainWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cfacet" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        FacetResult facet = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .on(q.department)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals("department", facet.fieldName());
        assertEquals(2L, facet.total());
        assertFalse(facet.buckets().isEmpty());
        assertEquals(departmentA, facet.buckets().get(0).value());
        assertEquals(2L, facet.buckets().get(0).count());

        FacetResult filteredFacet = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .on(q.department)
                .minCount(2)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals(1L, filteredFacet.total());
        assertEquals(1, filteredFacet.buckets().size());
        assertEquals(departmentA, filteredFacet.buckets().get(0).value());
        assertEquals(2L, filteredFacet.buckets().get(0).count());
    }

    @Test
    void facetIncludeValuesWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cfacetinc" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        FacetResult facet = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .on(q.department)
                .includeValues(departmentB)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals(1L, facet.total());
        assertEquals(1, facet.buckets().size());
        assertEquals(departmentB, facet.buckets().get(0).value());
        assertEquals(1L, facet.buckets().get(0).count());
    }

    @Test
    void multiFieldFacetResultsWorkAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cfacets" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        FacetResults facets = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .on(q.department, q.status)
                .size(10)
                .results();

        assertEquals(2, facets.facets().size());
        FacetResult departmentFacet = facets.facet("department");
        FacetResult statusFacet = facets.facet("status");
        assertTrue(departmentFacet != null && !departmentFacet.buckets().isEmpty());
        assertTrue(statusFacet != null && !statusFacet.buckets().isEmpty());
        assertEquals(departmentA, departmentFacet.buckets().get(0).value());
        assertEquals(2L, departmentFacet.buckets().get(0).count());
    }

    @Test
    void applyAndFilterAreRejectedAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "caggaf" + UUID.randomUUID().toString().replace("-", "");
        String department = "Dept-" + token;

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token, department, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token, department, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, department, department, 2L);

        ValkeyQueryExecutionException error = assertThrows(ValkeyQueryExecutionException.class, () -> repository.aggregateChain()
                .where(q.department.eq(department))
                .groupBy(q.department)
                .count("count")
                .apply("@count * 10", "weighted_count")
                .filter("@weighted_count > 10")
                .result());
        assertTrue(error.getMessage().contains("不支持 APPLY/FILTER"));
    }

    @Test
    void countDistinctIsRejectedAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "caggcd" + UUID.randomUUID().toString().replace("-", "");
        String department = "Dept-" + token;

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token, department, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token, department, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, department, department, 2L);

        ValkeyQueryExecutionException error = assertThrows(ValkeyQueryExecutionException.class, () -> repository.aggregateChain()
                .where(q.department.eq(department))
                .countDistinct(q.department, "department_count")
                .result());
        assertTrue(error.getMessage().contains("COUNT_DISTINCT"));
    }

    @Test
    void distinctWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cdistinct" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        List<String> departments = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .distinct(q.department);

        assertEquals(2, departments.size());
        assertTrue(departments.contains(departmentA));
        assertTrue(departments.contains(departmentB));
    }

    @Test
    void countDistinctShortcutWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "ccountdistinct" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        long departmentCount = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .countDistinct(q.department);

        assertEquals(2L, departmentCount);
    }

    @Test
    void distinctRowsWorkAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cdistinctrows" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        List<String> pairs = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .distinctRows(row -> row.getString("department") + ":" + row.getString("status"),
                        q.department,
                        q.status);

        assertEquals(2, pairs.size());
        assertTrue(pairs.contains(departmentA + ":ON"));
        assertTrue(pairs.contains(departmentB + ":OFF"));
    }

    @Test
    void distinctRowsTypeMappingWorksAgainstRealClusterValkey() throws Exception {
        HashStudentClusterRepository repository = new HashStudentClusterRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "cdistincttype" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        List<DepartmentStatusPair> rows = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .distinctRows(DepartmentStatusPair.class, q.department, q.status);

        assertEquals(2, rows.size());
        assertTrue(rows.stream().anyMatch(row -> departmentA.equals(row.department()) && "ON".equals(row.status())));
        assertTrue(rows.stream().anyMatch(row -> departmentB.equals(row.department()) && "OFF".equals(row.status())));
    }

    private void awaitIndexed(HashStudentClusterRepository repository, StudentQuery q, String departmentA, String departmentB, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = repository.queryChain()
                    .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                    .count();
            if (total >= expected) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("cluster aggregate 测试文档未在超时时间内进入索引");
    }

    record DepartmentStats(
            String department,
            long count,
            double score_sum,
            double score_avg,
            long age_min,
            long age_max
    ) {
    }

    record DepartmentStatusPair(String department, String status) {
    }

    private static final class HashStudentClusterRepository extends BaseValkeyRepository<Student> {

        private HashStudentClusterRepository(ValkeyClientRouting routing, ObjectMapper objectMapper) {
            super(StudentQuery.METADATA, routing, Student.class, objectMapper);
        }

        private void saveStudent(String id, String name, int age, double score, String className, String department, String status) {
            Student student = new Student();
            student.setId(Math.abs((long) id.hashCode()));
            student.setName(name);
            student.setAge(age);
            student.setScore(score);
            student.setClassName(className);
            student.setDepartment(department);
            student.setStatus(status);
            save(id, student);
        }
    }

    private static String getenv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Integer getenvInt(String key, int fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value);
    }
}
