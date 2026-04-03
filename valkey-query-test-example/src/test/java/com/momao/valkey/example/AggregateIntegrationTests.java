package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.DefaultValkeyClientRouting;
import com.momao.valkey.core.AggregateExpressions;
import com.momao.valkey.core.AggregateResult;
import com.momao.valkey.core.FacetResult;
import com.momao.valkey.core.FacetResults;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import glide.api.GlideClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class AggregateIntegrationTests {

    @Autowired
    private GlideClient glideClient;

    @Test
    void aggregateChainAndMapRowsRunAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "agg" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");
        repository.saveStudent(token + "4", "Dave-" + token, 21, 60.0d, "Class-" + token + "-2", departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 4L);

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
        List<DepartmentStats> mappedByFunction = result.mapRows(row -> new DepartmentStats(
                row.getString("department"),
                row.getLong("count"),
                row.getDouble("score_sum"),
                row.getDouble("score_avg"),
                row.getLong("age_min"),
                row.getLong("age_max")
        ));
        List<DepartmentStats> mappedByType = result.mapRows(DepartmentStats.class);

        DepartmentStats departmentAStats = mappedByFunction.stream()
                .filter(row -> departmentA.equals(row.department()))
                .findFirst()
                .orElseThrow();
        DepartmentStats departmentBStats = mappedByType.stream()
                .filter(row -> departmentB.equals(row.department()))
                .findFirst()
                .orElseThrow();

        assertEquals(2L, departmentAStats.count());
        assertEquals(170.0d, departmentAStats.score_sum());
        assertEquals(85.0d, departmentAStats.score_avg());
        assertEquals(18L, departmentAStats.age_min());
        assertEquals(19L, departmentAStats.age_max());
        assertEquals(1L, departmentBStats.count());
        assertEquals(70.0d, departmentBStats.score_sum());
        assertEquals(70.0d, departmentBStats.score_avg());
        assertEquals(20L, departmentBStats.age_min());
        assertEquals(20L, departmentBStats.age_max());
    }

    @Test
    void facetChainAndApplyFilterRunAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "facet" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";
        String classOne = "Class-" + token + "-1";
        String classTwo = "Class-" + token + "-2";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, classOne, departmentA, "ON");
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, classOne, departmentA, "ON");
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, classTwo, departmentB, "ON");
        repository.saveStudent(token + "4", "Dave-" + token, 21, 60.0d, classTwo, departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 4L);

        AggregateResult aggregate = repository.aggregateChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .groupBy(q.department)
                .count("count")
                .apply(AggregateExpressions.field("count").multiply(10), "weighted_count")
                .filter(AggregateExpressions.field("weighted_count").gt(10))
                .sortByDesc("weighted_count")
                .result();

        assertEquals(1L, aggregate.total());
        assertEquals(departmentA, aggregate.rows().get(0).getString("department"));
        assertEquals(20L, aggregate.rows().get(0).getLong("weighted_count"));

        FacetResult singleFacet = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .on(q.department)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals("department", singleFacet.fieldName());
        assertEquals(2L, singleFacet.total());
        assertFalse(singleFacet.buckets().isEmpty());
        assertEquals(departmentA, singleFacet.buckets().get(0).value());
        assertEquals(2L, singleFacet.buckets().get(0).count());

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

        FacetResult includedFacet = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .on(q.department)
                .includeValues(departmentB)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals(1L, includedFacet.total());
        assertEquals(1, includedFacet.buckets().size());
        assertEquals(departmentB, includedFacet.buckets().get(0).value());
        assertEquals(1L, includedFacet.buckets().get(0).count());

        FacetResults facets = repository.facetChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .on(q.department, q.status)
                .size(10)
                .results();

        assertEquals(2, facets.facets().size());
        assertNotNull(facets.facet("department"));
        assertNotNull(facets.facet("status"));
    }

    @Test
    void countDistinctWorksAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "aggdistinct" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";
        String classOne = "Class-" + token + "-1";
        String classTwo = "Class-" + token + "-2";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, classOne, departmentA, "ON");
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, classTwo, departmentA, "ON");
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, classTwo, departmentB, "ON");
        repository.saveStudent(token + "4", "Dave-" + token, 21, 60.0d, classTwo, departmentB, "OFF");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 4L);

        AggregateResult result = repository.aggregateChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON").or(q.status.eq("OFF")))
                .groupBy(q.status)
                .countDistinct(q.department, "department_count")
                .count("count")
                .sortByDesc("count")
                .result();

        assertEquals(2L, result.total());
        assertEquals("ON", result.rows().get(0).getString("status"));
        assertEquals(2L, result.rows().get(0).getLong("department_count"));
        assertEquals(3L, result.rows().get(0).getLong("count"));
        assertEquals("OFF", result.rows().get(1).getString("status"));
        assertEquals(1L, result.rows().get(1).getLong("department_count"));
        assertEquals(1L, result.rows().get(1).getLong("count"));
    }

    @Test
    void distinctWorksAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "aggdistinctq" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, "ON");
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, "ON");

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
    void countDistinctShortcutWorksAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "aggcountdistinctq" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";
        String statusOn = "ON";
        String statusOff = "OFF";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, statusOn);
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, statusOn);
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, statusOff);

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        long departmentCount = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .countDistinct(q.department);

        assertEquals(2L, departmentCount);
    }

    @Test
    void distinctRowsWorkAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "aggdistinctrows" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";
        String statusOn = "ON";
        String statusOff = "OFF";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, statusOn);
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, statusOn);
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, statusOff);

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        List<String> pairs = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .distinctRows(row -> row.getString("department") + ":" + row.getString("status"), q.department, q.status);

        assertEquals(2, pairs.size());
        assertTrue(pairs.contains(departmentA + ":" + statusOn));
        assertTrue(pairs.contains(departmentB + ":" + statusOff));
    }

    @Test
    void distinctRowsTypeMappingWorksAgainstRealValkey() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "aggdistincttype" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";
        String statusOn = "ON";
        String statusOff = "OFF";

        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, statusOn);
        repository.saveStudent(token + "2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-2", departmentA, statusOn);
        repository.saveStudent(token + "3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-3", departmentB, statusOff);

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, departmentA, departmentB, 3L);

        List<DepartmentStatusPair> rows = repository.queryChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .distinctRows(DepartmentStatusPair.class, q.department, q.status);

        assertEquals(2, rows.size());
        assertTrue(rows.stream().anyMatch(row -> departmentA.equals(row.department()) && statusOn.equals(row.status())));
        assertTrue(rows.stream().anyMatch(row -> departmentB.equals(row.department()) && statusOff.equals(row.status())));
    }

    @Test
    void textFieldFacetFailsAgainstRealValkeyAndDocumentsBoundary() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = "textfacet" + UUID.randomUUID().toString().replace("-", "");
        String department = "Dept-" + token;
        repository.saveStudent(token + "1", "Alice-" + token, 18, 90.0d, "ClassA" + token, department, "ON");

        StudentQuery q = new StudentQuery();
        awaitIndexed(repository, q, department, department, 1L);

        ValkeyQueryExecutionException error = assertThrows(ValkeyQueryExecutionException.class, () -> repository.facetChain()
                .where(q.status.eq("ON"))
                .on(q.className)
                .result());

        boolean found = false;
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("Invalid data type")) {
                found = true;
                break;
            }
            current = current.getCause();
        }
        assertTrue(found);
    }

    private void awaitIndexed(HashStudentRepository repository, StudentQuery q, String departmentA, String departmentB, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = repository.queryChain()
                    .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                    .count();
            if (total >= expected) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("aggregate 测试文档未在超时时间内进入索引");
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

    private static final class HashStudentRepository extends BaseValkeyRepository<Student> {

        private HashStudentRepository(GlideClient glideClient) {
            super(
                    StudentQuery.METADATA,
                    new DefaultValkeyClientRouting(glideClient),
                    Student.class,
                    new ObjectMapper().findAndRegisterModules()
            );
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
}
