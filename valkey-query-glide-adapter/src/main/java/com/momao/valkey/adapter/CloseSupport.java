package com.momao.valkey.adapter;

final class CloseSupport {

    private CloseSupport() {
    }

    static void closeAll(AutoCloseable... closeables) throws Exception {
        Exception firstFailure = null;
        for (AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }
            try {
                closeable.close();
            } catch (Exception exception) {
                if (firstFailure == null) {
                    firstFailure = exception;
                } else {
                    firstFailure.addSuppressed(exception);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }
}
