package com.momao.valkey.adapter;

record CommandBatch<I>(
        I item,
        String[] command
) {
}
