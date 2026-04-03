package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.IndexDiff;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.core.exception.ValkeyConfigurationException;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.metadata.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ValkeyIndexManager {

    private static final Logger log = LoggerFactory.getLogger(ValkeyIndexManager.class);

    private final ValkeyClientRouting clientRouting;
    private final ValkeyQueryPackages packages;
    private final ValkeyQueryProperties properties;
    private final ValkeyIndexMetrics indexMetrics;

    public ValkeyIndexManager(
            ValkeyClientRouting clientRouting,
            ValkeyQueryPackages packages,
            ValkeyQueryProperties properties,
            ValkeyIndexMetrics indexMetrics) {
        this.clientRouting = clientRouting;
        this.packages = packages;
        this.properties = properties;
        this.indexMetrics = indexMetrics == null ? ValkeyIndexMetrics.noop() : indexMetrics;
    }

    public void manageApplicationIndexes(ClassLoader classLoader) {
        if (properties.getIndexManagement().getMode() == IndexManagementMode.NONE) {
            return;
        }
        for (IndexSchema schema : loadApplicationSchemas(classLoader)) {
            applyIndexManagement(repository(schema));
        }
    }

    public void applyIndexManagement(BaseValkeyRepository<?> repository) {
        IndexManagementMode mode = properties.getIndexManagement().getMode();
        switch (mode) {
            case NONE -> {
            }
            case VALIDATE -> validate(repository);
            case RECREATE -> recreate(repository);
        }
    }

    public List<ValkeyManagedIndexPlan> inspectApplicationPlans(ClassLoader classLoader) {
        if (properties.getIndexManagement().getMode() == IndexManagementMode.NONE) {
            return List.of();
        }
        List<ValkeyManagedIndexPlan> plans = new ArrayList<>();
        for (IndexSchema schema : loadApplicationSchemas(classLoader)) {
            plans.add(inspectPlan(schema));
        }
        return plans;
    }

    public ValkeyManagedIndexPlan inspectPlan(IndexSchema schema) {
        BaseValkeyRepository<?> repository = repository(schema);
        return inspectPlan(repository);
    }

    public ValkeyManagedIndexPlan inspectPlan(BaseValkeyRepository<?> repository) {
        IndexDiff diff = repository.inspectIndexDiff();
        indexMetrics.recordDiff(diff, properties.getIndexManagement().getMode());
        return new ValkeyManagedIndexPlan(repository.getSchema(), diff.plan());
    }

    public String applyPlan(ValkeyManagedIndexPlan managedPlan) {
        BaseValkeyRepository<?> repository = repository(managedPlan.schema());
        return applyPlan(managedPlan, repository);
    }

    String applyPlan(ValkeyManagedIndexPlan managedPlan, BaseValkeyRepository<?> repository) {
        return switch (managedPlan.plan().action()) {
            case NONE -> "OK";
            case CREATE -> repository.createIndex();
            case RECREATE -> {
                dropIndexQuietly(repository, true);
                indexMetrics.recordRecreate(managedPlan.schema().indexName());
                yield repository.createIndex();
            }
        };
    }

    private List<IndexSchema> loadApplicationSchemas(ClassLoader classLoader) {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(ValkeyDocument.class));

        Set<String> createdIndexes = new LinkedHashSet<>();
        List<IndexSchema> schemas = new ArrayList<>();
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
                schemas.add(schema);
            }
        }
        return schemas;
    }

    private void validate(BaseValkeyRepository<?> repository) {
        IndexDiff diff = repository.inspectIndexDiff();
        indexMetrics.recordDiff(diff, properties.getIndexManagement().getMode());
        if (diff.isEmpty()) {
            return;
        }
        indexMetrics.recordValidationFailure(repository.getIndexName(), "INDEX_002", "INDEX");
        log.error("Valkey 索引校验失败，diff={}", diff.summary());
        throw new IllegalStateException("索引不一致，VALIDATE 模式已阻止启动，请检查 schema 或手动重建索引。");
    }

    private void recreate(BaseValkeyRepository<?> repository) {
        dropIndexQuietly(repository, true);
        indexMetrics.recordRecreate(repository.getIndexName());
        repository.createIndex();
    }

    private BaseValkeyRepository<?> repository(IndexSchema schema) {
        return new BaseValkeyRepository<>(schema, clientRouting, Object.class, null) { };
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
            throw new ValkeyConfigurationException(ValkeyErrorCode.CONFIG_METADATA_LOAD_FAILED, "无法读取实体索引元数据: " + entityClassName, exception);
        }
    }

    private void dropIndexQuietly(BaseValkeyRepository<?> repository, boolean deleteDocuments) {
        try {
            repository.dropIndex(deleteDocuments);
        } catch (Exception exception) {
            if (!isMissingIndexError(exception)) {
                throw exception;
            }
        }
    }

    private boolean isMissingIndexError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("Unknown Index name") || message.contains("unknown index") || message.contains("no such index"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
