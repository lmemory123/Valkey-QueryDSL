package com.momao.valkey.adapter;

import com.momao.valkey.core.BulkMode;
import glide.api.GlideClient;
import glide.api.models.GlideString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class DefaultValkeyClientRouting implements ValkeyClientRouting {

    private final GlideClient writeClient;

    private final GlideClient readClient;

    private final boolean closeReadClient;

    public DefaultValkeyClientRouting(GlideClient writeClient) {
        this(writeClient, writeClient, false);
    }

    public DefaultValkeyClientRouting(GlideClient writeClient, GlideClient readClient, boolean closeReadClient) {
        this.writeClient = writeClient;
        this.readClient = readClient == null ? writeClient : readClient;
        this.closeReadClient = closeReadClient && this.readClient != null && this.readClient != this.writeClient;
    }

    @Override
    public Object executeWrite(String[] command) throws Exception {
        return writeClient.customCommand(command).get();
    }

    @Override
    public Object executeRead(String[] command) throws Exception {
        return readClient.customCommand(command).get();
    }

    @Override
    public Object executeRead(GlideString[] command) throws Exception {
        return readClient.customCommand(command).get();
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
}
