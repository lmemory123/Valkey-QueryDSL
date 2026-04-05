package com.momao.valkey.autoconfigure;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.type.AnnotationMetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValkeyQueryScannerRegistrarTests {

    @Test
    void enableValkeyQuerySupportsValueAlias() {
        DefaultListableBeanFactory registry = new DefaultListableBeanFactory();

        new ValkeyQueryScannerRegistrar().registerBeanDefinitions(
                AnnotationMetadata.introspect(ValueConfiguredApplication.class),
                registry
        );

        ValkeyQueryPackages packages = (ValkeyQueryPackages) registry.getBean(ValkeyQueryPackages.class.getName());
        assertEquals("com.example.value", packages.basePackages().get(0));
    }

    @EnableValkeyQuery("com.example.value")
    static class ValueConfiguredApplication {
    }
}
