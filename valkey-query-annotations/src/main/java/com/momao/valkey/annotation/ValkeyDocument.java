package com.momao.valkey.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ValkeyDocument {

    String DEFAULT_QUERY_SUFFIX = "Query";

    String value() default "";

    String indexName() default "";

    @Deprecated(since = "1.0.0")
    String prefix() default "";

    String[] prefixes() default {};

    StorageType storageType() default StorageType.JSON;

    String querySuffix() default DEFAULT_QUERY_SUFFIX;
}