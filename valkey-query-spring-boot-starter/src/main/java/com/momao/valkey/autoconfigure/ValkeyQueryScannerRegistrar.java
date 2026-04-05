package com.momao.valkey.autoconfigure;

import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import java.util.Map;

public class ValkeyQueryScannerRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(@NonNull AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (registry.containsBeanDefinition(ValkeyQueryPackages.class.getName())) {
            return;
        }

        Map<String, Object> attributes = importingClassMetadata.getAnnotationAttributes(EnableValkeyQuery.class.getName(), false);
        String[] basePackages = attributes == null ? new String[0] : (String[]) attributes.get("basePackages");
        if ((basePackages == null || basePackages.length == 0) && attributes != null) {
            basePackages = (String[]) attributes.get("value");
        }
        if (basePackages == null || basePackages.length == 0) {
            basePackages = new String[]{ClassUtils.getPackageName(importingClassMetadata.getClassName())};
        }

        RootBeanDefinition definition = new RootBeanDefinition(ValkeyQueryPackages.class);
        definition.getConstructorArgumentValues().addIndexedArgumentValue(0, basePackages);
        registry.registerBeanDefinition(ValkeyQueryPackages.class.getName(), definition);
    }
}
