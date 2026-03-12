package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.core.metadata.IndexSchema;
import glide.api.GlideClient;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Field;
import java.util.LinkedHashSet;
import java.util.Set;

public class ValkeyIndexAutoCreator implements ApplicationListener<ApplicationReadyEvent> {

    private final GlideClient glideClient;

    private final ValkeyQueryPackages packages;

    public ValkeyIndexAutoCreator(GlideClient glideClient, ValkeyQueryPackages packages) {
        this.glideClient = glideClient;
        this.packages = packages;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(ValkeyDocument.class));

        Set<String> createdIndexes = new LinkedHashSet<>();
        ClassLoader classLoader = event.getApplicationContext().getClassLoader();
        for (String basePackage : packages.basePackages()) {
            for (BeanDefinition candidate : provider.findCandidateComponents(basePackage)) {
                String candidateClassName = candidate.getBeanClassName();
                if (candidateClassName == null) {
                    continue;
                }
                IndexSchema schema = loadMetadata(candidateClassName, classLoader);
                if (schema == null || !createdIndexes.add(schema.indexName())) {
                    continue;
                }
                new BaseValkeyRepository<>(schema, glideClient, Object.class, null) { }.checkAndCreateIndex();
            }
        }
    }

    private IndexSchema loadMetadata(String entityClassName, ClassLoader classLoader) {
        try {
            Class<?> entityClass = ClassUtils.forName(entityClassName, classLoader);
            ValkeyDocument document = entityClass.getAnnotation(ValkeyDocument.class);
            String querySuffix = document == null || document.querySuffix().isBlank()
                    ? ValkeyDocument.DEFAULT_QUERY_SUFFIX
                    : document.querySuffix();
            String queryClassName = entityClass.getPackageName() + "." + entityClass.getSimpleName() + querySuffix;
            Class<?> queryClass = ClassUtils.forName(queryClassName, classLoader);
            Field metadataField = queryClass.getField("METADATA");
            return (IndexSchema) metadataField.get(null);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("无法读取实体索引元数据: " + entityClassName, exception);
        }
    }
}
