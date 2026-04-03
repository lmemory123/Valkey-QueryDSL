package com.momao.valkey.adapter;

import com.momao.valkey.core.BulkMode;
import glide.api.models.GlideString;

import java.util.ArrayList;
import java.util.List;

public interface ValkeyClientRouting extends AutoCloseable {

    Object executeWrite(String[] command) throws Exception;

    Object executeRead(String[] command) throws Exception;

    default Object executeRead(GlideString[] command) throws Exception {
        throw new UnsupportedOperationException("Binary read commands are not implemented");
    }

    default Object executeIndex(String[] command) throws Exception {
        return executeWrite(command);
    }

    default List<Object> executeReadAll(String[] command) throws Exception {
        return List.of(executeRead(command));
    }

    default List<Object> executeIndexAll(String[] command) throws Exception {
        return List.of(executeIndex(command));
    }

    default List<BatchCommandResult> executeWriteBatch(List<String[]> commands, BulkMode mode) {
        if (commands == null || commands.isEmpty()) {
            return List.of();
        }
        List<BatchCommandResult> results = new ArrayList<>(commands.size());
        BulkMode resolvedMode = mode == null ? BulkMode.ORDERED : mode;
        for (String[] command : commands) {
            try {
                results.add(BatchCommandResult.success(executeWrite(command)));
            } catch (Exception exception) {
                results.add(BatchCommandResult.failure(exception));
                if (resolvedMode == BulkMode.ORDERED) {
                    break;
                }
            }
        }
        return results;
    }

    default boolean isClusterMode() {
        return false;
    }

    @Override
    default void close() throws Exception {
    }
}
