package com.momao.valkey.core;

import java.util.regex.Pattern;

public final class ValkeySyntaxUtils {

    private static final Pattern SPECIAL_CHARS = Pattern.compile("([,.<>{}\\[\\]\"':;!@#$%^&*()\\-+=~/\\\\| ])");

    private ValkeySyntaxUtils() {
    }

    public static String escape(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return SPECIAL_CHARS.matcher(value).replaceAll("\\\\$1");
    }

    public static String escapeTag(String value) {
        return escape(value);
    }

    public static String escapeWildcard(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\"", "\\\"");
    }
}
