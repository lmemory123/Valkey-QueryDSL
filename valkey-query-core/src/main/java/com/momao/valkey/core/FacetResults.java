package com.momao.valkey.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record FacetResults(
        List<FacetResult> facets
) {

    public FacetResults {
        facets = facets == null ? List.of() : List.copyOf(facets);
    }

    public FacetResult facet(String fieldName) {
        if (fieldName == null || fieldName.isBlank()) {
            return null;
        }
        for (FacetResult facet : facets) {
            if (fieldName.equals(facet.fieldName())) {
                return facet;
            }
        }
        return null;
    }

    public Map<String, FacetResult> asMap() {
        Map<String, FacetResult> mapped = new LinkedHashMap<>();
        for (FacetResult facet : facets) {
            mapped.put(facet.fieldName(), facet);
        }
        return mapped;
    }
}
