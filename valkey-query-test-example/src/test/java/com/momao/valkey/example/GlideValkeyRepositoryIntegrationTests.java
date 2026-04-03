package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.DefaultValkeyClientRouting;
import com.momao.valkey.core.Page;
import glide.api.GlideClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class GlideValkeyRepositoryIntegrationTests {

    @Autowired
    private GlideClient glideClient;

    @Autowired
    private SkuRepository skuRepository;

    @Test
    void savesJsonAndQueriesThroughValkey() throws Exception {
        String token = "it" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "IntegrationPro" + token,
                2999,
                List.of("HOT", "NEW"),
                new Merchant("Apple Flagship", "S"));

        String ping = glideClient.ping().get();
        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitIndexed(q, token);
        Sku loaded = skuRepository.findById(token);
        Sku matched = skuRepository.queryChain()
            .where(q.title.eq("IntegrationPro" + token).and(
                q.merchant.name.eq("Apple Flagship"
                )
            ))
            .one();
        Page<Sku> result = skuRepository.queryChain()
            .where(q.id.eq(token))
            .and(q.tags.contains("HOT"))
            .orderByAsc("price")
            .page(0, 10);
        long total = skuRepository.queryChain()
            .where(q.id.eq(token))
            .count();

        System.out.println("[integration] ping=" + ping);
        System.out.println("[integration] key=" + skuRepository.getPrefix() + token);
        System.out.println("[integration] query=" + q.id.eq(token).and(q.tags.contains("HOT")).build());
        System.out.println("[integration] total=" + total);
        System.out.println("[integration] records=" + result.records());

        assertEquals("PONG", ping);
        assertEquals(token, loaded.getId());
        assertEquals("IntegrationPro" + token, loaded.getTitle());
        assertNotNull(matched);
        assertEquals(token, matched.getId());
        assertEquals(1L, total);
        assertTrue(result.total() >= 1);
        assertTrue(result.records().stream().anyMatch(item -> token.equals(item.getId())));
        assertEquals(2999, matched.getPrice());
        assertEquals(List.of("HOT", "NEW"), matched.getTags());
        assertEquals("Apple Flagship", matched.getMerchant().getName());
    }

    @Test
    void updateChainUpdatesJsonFieldsWithoutOverwritingWholeDocument() throws Exception {
        String token = "up" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "UpdateTarget" + token,
                2599,
                List.of("HOT", "KEEP"),
                new Merchant("Before Merchant", "A"));

        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitIndexed(q, token);
        long updated = skuRepository.updateChain()
                .where(q.id.eq(token))
                .set(q.price, 3199)
                .set(q.merchant.name, "After Merchant")
                .set(q.tags, List.of("HOT", "UPDATED"))
                .execute();
        awaitIndexedCondition(q.id.eq(token).and(q.merchant.name.eq("After Merchant")));

        Sku loaded = skuRepository.findById(token);

        assertEquals(1L, updated);
        assertNotNull(loaded);
        assertEquals(token, loaded.getId());
        assertEquals("UpdateTarget" + token, loaded.getTitle());
        assertEquals(3199, loaded.getPrice());
        assertEquals(List.of("HOT", "UPDATED"), loaded.getTags());
        assertEquals("After Merchant", loaded.getMerchant().getName());
        assertEquals("A", loaded.getMerchant().getLevel());
    }

    @Test
    void updateChainSupportsOptimisticCompareAndSetForJsonFields() throws Exception {
        String token = "cas" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "Versioned" + token,
                2599,
                List.of("HOT", "CAS"),
                new Merchant("Before Merchant", "B"));

        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitIndexed(q, token);

        long first = skuRepository.updateChain()
                .where(q.id.eq(token))
                .and(q.price.eq(2599))
                .and(q.merchant.name.eq("Before Merchant"))
                .set(q.price, 3199)
                .set(q.merchant.name, "After Merchant")
                .execute();

        long second = skuRepository.updateChain()
                .where(q.id.eq(token))
                .and(q.price.eq(2599))
                .set(q.price, 3999)
                .execute();

        awaitIndexedCondition(q.id.eq(token).and(q.price.eq(3199)));
        Sku loaded = skuRepository.findById(token);

        assertEquals(1L, first);
        assertEquals(0L, second);
        assertNotNull(loaded);
        assertEquals(3199, loaded.getPrice());
        assertEquals("After Merchant", loaded.getMerchant().getName());
    }

    @Test
    void updateChainAtomicallyIncrementsJsonNumericFieldUnderConcurrency() throws Exception {
        String token = "inc" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "Concurrent" + token,
                1000,
                List.of("HOT"),
                new Merchant("Concurrency Merchant", "A"));
        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitIndexed(q, token);
        runConcurrently(24, () -> skuRepository.updateChain()
                .whereId(token)
                .increment(q.price, 1)
                .execute());
        awaitIndexedCondition(q.id.eq(token).and(q.price.eq(1024)));

        Sku loaded = skuRepository.findById(token);
        assertNotNull(loaded);
        assertEquals(1024, loaded.getPrice());
    }

    @Test
    void updateChainAtomicallyIncrementsHashNumericFieldUnderConcurrency() throws Exception {
        HashStudentRepository repository = new HashStudentRepository(glideClient);
        repository.checkAndCreateIndex();

        String token = Long.toString(System.nanoTime());
        Student student = new Student();
        student.setId(Long.valueOf(token));
        student.setName("Student" + token);
        student.setAge(18);
        student.setScore(90.5d);
        student.setClassName("Class A");
        student.setDepartment("Backend");
        student.setStatus("ON");
        repository.save(token, student);

        StudentQuery q = new StudentQuery();
        runConcurrently(20, () -> repository.updateChain()
                .whereId(token)
                .increment(q.age, 1)
                .execute());

        Object age = glideClient.customCommand(new String[]{"HGET", repository.getPrefix() + token, "age"}).get();
        assertEquals("38", String.valueOf(age));
    }

    private void awaitIndexed(SkuQuery q, String token) throws Exception {
        awaitIndexedCondition(q.id.eq(token));
    }

    private void awaitIndexedCondition(com.momao.valkey.core.SearchCondition condition) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = skuRepository.queryChain()
                .where(condition)
                .count();
            if (total > 0) {
                return;
            }
            Thread.sleep(100L);
        }
        fail("文档已写入 JSON，但在超时时间内未进入搜索索引: " + condition.build());
    }

    private void runConcurrently(int taskCount, CheckedRunnable runnable) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(taskCount);
        CountDownLatch ready = new CountDownLatch(taskCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(taskCount);
        List<Throwable> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        try {
            for (int i = 0; i < taskCount; i++) {
                executorService.submit(() -> {
                    ready.countDown();
                    try {
                        if (!start.await(5, TimeUnit.SECONDS)) {
                            failures.add(new IllegalStateException("并发测试启动超时"));
                            return;
                        }
                        runnable.run();
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    } finally {
                        done.countDown();
                    }
                });
            }
            if (!ready.await(5, TimeUnit.SECONDS)) {
                fail("并发测试线程未准备完成");
            }
            start.countDown();
            if (!done.await(10, TimeUnit.SECONDS)) {
                fail("并发测试线程未在超时时间内完成");
            }
            if (!failures.isEmpty()) {
                Throwable first = failures.get(0);
                if (first instanceof Exception exception) {
                    throw exception;
                }
                throw new RuntimeException(first);
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
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
    }
}
