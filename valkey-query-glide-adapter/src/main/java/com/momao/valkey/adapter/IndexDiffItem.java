package com.momao.valkey.adapter;

public record IndexDiffItem(
        IndexDiffType type,
        String target,
        String expected,
        String actual
) {

    public String summary() {
        return "type=" + type
                + ",target=" + blankToDash(target)
                + ",expected=" + blankToDash(expected)
                + ",actual=" + blankToDash(actual);
    }

    private String blankToDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
