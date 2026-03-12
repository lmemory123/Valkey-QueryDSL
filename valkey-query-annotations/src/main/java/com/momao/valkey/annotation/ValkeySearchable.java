package com.momao.valkey.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ValkeySearchable {

    String value() default "";

    double weight() default 1.0;

    boolean noStem() default false;

    boolean sortable() default false;
}
