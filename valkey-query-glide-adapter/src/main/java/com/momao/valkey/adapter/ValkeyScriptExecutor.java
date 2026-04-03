package com.momao.valkey.adapter;

import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

final class ValkeyScriptExecutor {

    static final int DEFAULT_MAX_CACHED_SCRIPTS = 64;

    static final ValkeyScriptExecutor DEFAULT = new ValkeyScriptExecutor();

    private final Map<String, String> shaByScript;

    ValkeyScriptExecutor() {
        this(DEFAULT_MAX_CACHED_SCRIPTS);
    }

    ValkeyScriptExecutor(int maxCachedScripts) {
        if (maxCachedScripts <= 0) {
            throw new IllegalArgumentException("maxCachedScripts must be greater than 0");
        }
        this.shaByScript = java.util.Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > maxCachedScripts;
            }
        });
    }

    Object executeWrite(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            Supplier<String> description,
            String script,
            int keyCount,
            List<String> keysAndArgs) throws Exception {
        String sha = resolveSha(script);
        try {
            return observationInvoker.execute(
                    "EVALSHA",
                    indexName,
                    description,
                    "write",
                    () -> clientRouting.executeWrite(buildEvalShaCommand(sha, keyCount, keysAndArgs))
            );
        } catch (Exception exception) {
            if (!isNoScript(exception)) {
                throw exception;
            }
        }
        return observationInvoker.execute(
                "EVAL",
                indexName,
                description,
                "write",
                () -> clientRouting.executeWrite(buildEvalCommand(script, keyCount, keysAndArgs))
        );
    }

    int cacheSize() {
        synchronized (shaByScript) {
            return shaByScript.size();
        }
    }

    boolean isCached(String script) {
        synchronized (shaByScript) {
            return shaByScript.containsKey(script);
        }
    }

    private String resolveSha(String script) {
        synchronized (shaByScript) {
            String cached = shaByScript.get(script);
            if (cached != null) {
                return cached;
            }
            String computed = sha1Hex(script);
            shaByScript.put(script, computed);
            return computed;
        }
    }

    private void cacheSha(String script, String sha) {
        synchronized (shaByScript) {
            shaByScript.put(script, sha);
        }
    }

    private static String[] buildEvalShaCommand(String sha, int keyCount, List<String> keysAndArgs) {
        List<String> command = new ArrayList<>(3 + keysAndArgs.size());
        command.add("EVALSHA");
        command.add(sha);
        command.add(Integer.toString(keyCount));
        command.addAll(keysAndArgs);
        return command.toArray(String[]::new);
    }

    private static String[] buildEvalCommand(String script, int keyCount, List<String> keysAndArgs) {
        List<String> command = new ArrayList<>(3 + keysAndArgs.size());
        command.add("EVAL");
        command.add(script);
        command.add(Integer.toString(keyCount));
        command.addAll(keysAndArgs);
        return command.toArray(String[]::new);
    }

    private static boolean isNoScript(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("NOSCRIPT")
                    || message.contains("NoScriptError")
                    || message.contains("No matching script"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String sha1Hex(String script) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] hash = digest.digest(script.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                builder.append(String.format("%02x", value));
            }
            return builder.toString();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to compute SHA1 for Valkey script", exception);
        }
    }
}
