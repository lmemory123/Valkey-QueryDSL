package com.momao.valkey.autoconfigure;

import java.util.Arrays;
import java.util.List;

public record ValkeyQueryPackages(List<String> basePackages) {

    public ValkeyQueryPackages(String[] basePackages) {
        this(Arrays.stream(basePackages)
                .filter(pkg -> pkg != null && !pkg.isBlank())
                .distinct()
                .toList());
    }
}