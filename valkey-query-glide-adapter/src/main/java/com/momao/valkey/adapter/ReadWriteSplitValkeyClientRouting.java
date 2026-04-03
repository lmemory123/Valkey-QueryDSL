package com.momao.valkey.adapter;

import com.momao.valkey.core.BulkMode;
import com.momao.valkey.core.ReadPreference;
import glide.api.GlideClient;
import glide.api.models.GlideString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class ReadWriteSplitValkeyClientRouting implements ValkeyClientRouting {

    private final GlideClient writeClient;

    private final GlideClient readClient;

    private final ReadPreference readPreference;

    private final boolean closeReadClient;

    public ReadWriteSplitValkeyClientRouting(
            GlideClient writeClient,
            GlideClient readClient,
            ReadPreference readPreference,
            boolean closeReadClient) {
        this.writeClient = writeClient;
        this.readClient = readClient == null ? writeClient : readClient;
        this.readPreference = readPreference == null ? ReadPreference.PRIMARY : readPreference;
        this.closeReadClient = closeReadClient && this.readClient != null && this.readClient != this.writeClient;
    }

    @Override
    public Object executeWrite(String[] command) throws Exception {
        return writeClient.customCommand(command).get();
    }

    @Override
    public Object executeRead(String[] command) throws Exception {
        return switch (readPreference) {
            case PRIMARY -> executeOn(writeClient, command);
            case PRIMARY_PREFERRED -> executePrimaryPreferred(command);
            case REPLICA_PREFERRED -> executeReplicaPreferred(command);
        };
    }

    @Override
    public Object executeRead(GlideString[] command) throws Exception {
        return switch (readPreference) {
            case PRIMARY -> executeOn(writeClient, command);
            case PRIMARY_PREFERRED -> executePrimaryPreferred(command);
            case REPLICA_PREFERRED -> executeReplicaPreferred(command);
        };
    }

    @Override
    public Object executeIndex(String[] command) throws Exception {
        return executeWrite(command);
    }

    @Override
    public List<BatchCommandResult> executeWriteBatch(List<String[]> commands, BulkMode mode) {
        if (mode != BulkMode.UNORDERED || commands == null || commands.isEmpty()) {
            return ValkeyClientRouting.super.executeWriteBatch(commands, mode);
        }
        List<CompletableFuture<Object>> futures = new ArrayList<>(commands.size());
        List<BatchCommandResult> results = new ArrayList<>(commands.size());
        for (String[] command : commands) {
            try {
                futures.add(writeClient.customCommand(command));
                results.add(null);
            } catch (Exception exception) {
                futures.add(null);
                results.add(BatchCommandResult.failure(exception));
            }
        }
        for (int index = 0; index < commands.size(); index++) {
            if (results.get(index) != null) {
                continue;
            }
            CompletableFuture<Object> future = futures.get(index);
            try {
                results.set(index, BatchCommandResult.success(future.get()));
            } catch (Exception exception) {
                results.set(index, BatchCommandResult.failure(exception));
            }
        }
        return results;
    }

    @Override
    public void close() throws Exception {
        CloseSupport.closeAll(writeClient, closeReadClient ? readClient : null);
    }

    private Object executePrimaryPreferred(String[] command) throws Exception {
        try {
            return executeOn(writeClient, command);
        } catch (Exception exception) {
            if (!hasDedicatedReadClient()) {
                throw exception;
            }
            return executeOn(readClient, command);
        }
    }

    private Object executePrimaryPreferred(GlideString[] command) throws Exception {
        try {
            return executeOn(writeClient, command);
        } catch (Exception exception) {
            if (!hasDedicatedReadClient()) {
                throw exception;
            }
            return executeOn(readClient, command);
        }
    }

    private Object executeReplicaPreferred(String[] command) throws Exception {
        if (!hasDedicatedReadClient()) {
            return executeOn(writeClient, command);
        }
        try {
            return executeOn(readClient, command);
        } catch (Exception exception) {
            if (isSearchIndexUnavailable(exception, command)) {
                return executeOn(writeClient, command);
            }
            return executeOn(writeClient, command);
        }
    }

    private Object executeReplicaPreferred(GlideString[] command) throws Exception {
        if (!hasDedicatedReadClient()) {
            return executeOn(writeClient, command);
        }
        try {
            return executeOn(readClient, command);
        } catch (Exception exception) {
            if (isSearchIndexUnavailable(exception, command)) {
                return executeOn(writeClient, command);
            }
            return executeOn(writeClient, command);
        }
    }

    private Object executeOn(GlideClient client, String[] command) throws Exception {
        return client.customCommand(command).get();
    }

    private Object executeOn(GlideClient client, GlideString[] command) throws Exception {
        return client.customCommand(command).get();
    }

    private boolean hasDedicatedReadClient() {
        return readClient != null && readClient != writeClient;
    }

    private boolean isSearchIndexUnavailable(Throwable throwable, String[] command) {
        if (!isSearchCommand(command)) {
            return false;
        }
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("index with name")
                        || normalized.contains("unknown index name")
                        || normalized.contains("not found in database")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isSearchIndexUnavailable(Throwable throwable, GlideString[] command) {
        if (!isSearchCommand(command)) {
            return false;
        }
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String normalized = message.toLowerCase();
                if (normalized.contains("index with name")
                        || normalized.contains("unknown index name")
                        || normalized.contains("not found in database")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isSearchCommand(String[] command) {
        return command != null
                && command.length > 0
                && command[0] != null
                && command[0].regionMatches(true, 0, "FT.", 0, 3);
    }

    private boolean isSearchCommand(GlideString[] command) {
        return command != null
                && command.length > 0
                && command[0] != null
                && command[0].toString().regionMatches(true, 0, "FT.", 0, 3);
    }
}
