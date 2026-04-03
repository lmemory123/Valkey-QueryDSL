package com.momao.valkey.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ValkeyVector {

    String value() default "";

    int dimension();

    DistanceMetric distanceMetric() default DistanceMetric.COSINE;

    int m() default 16;

    int efConstruction() default 200;
}
