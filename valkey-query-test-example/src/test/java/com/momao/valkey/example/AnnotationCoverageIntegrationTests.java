package com.momao.valkey.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class AnnotationCoverageIntegrationTests {

    @Autowired
    private AnnotationCoverageRepository repository;

    @Test
    void explicitAndInferredIndexedTypesWorkAgainstRealValkey() throws Exception {
        repository.checkAndCreateIndex();

        String token = "ann" + UUID.randomUUID().toString().replace("-", "");
        AnnotationCoverageDocument entity = new AnnotationCoverageDocument(
                token,
                "hello-" + token,
                "PUBLIC",
                12,
                7L,
                "keyword" + token,
                List.of("HOT", token),
                new float[]{0.1f, 0.2f, 0.3f, 0.4f}
        );
        repository.save(token, entity);

        AnnotationCoverageDocumentSearchQuery q = new AnnotationCoverageDocumentSearchQuery();
        awaitIndexed(token, q);

        assertEquals(1L, repository.queryChain().where(q.status.eq("PUBLIC")).and(q.id.eq(token)).count());
        assertEquals(1L, repository.queryChain().where(q.playCount.eq(12)).and(q.id.eq(token)).count());
        assertEquals(1L, repository.queryChain().where(q.rank.eq(7L)).and(q.id.eq(token)).count());
        assertEquals(1L, repository.queryChain().where(q.keyword.contains("keyword" + token)).count());
        assertEquals(1L, repository.queryChain().where(q.labels.contains(token)).count());

        AnnotationCoverageDocument record = repository.queryChain()
                .where(q.id.eq(token))
                .one();
        assertNotNull(record);
        assertEquals("PUBLIC", record.getStatus());
        assertEquals(12, record.getPlayCount());
        assertEquals(7L, record.getRank());
        assertEquals("keyword" + token, record.getKeyword());
    }

    private void awaitIndexed(String token, AnnotationCoverageDocumentSearchQuery q) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (repository.queryChain().where(q.id.eq(token)).count() > 0) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("annotation coverage 文档未在超时时间内进入索引: " + token);
    }
}
