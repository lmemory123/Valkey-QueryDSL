package com.momao.valkey.adapter.observability;

@FunctionalInterface
public interface ValkeyCommandCallback<T> {

    T execute() throws Exception;
}
