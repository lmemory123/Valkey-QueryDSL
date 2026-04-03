package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.BaseValkeyRepository;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

public class ValkeyIndexAutoCreator implements ApplicationListener<ApplicationReadyEvent> {

    private final ValkeyIndexManager indexManager;

    public ValkeyIndexAutoCreator(ValkeyIndexManager indexManager) {
        this.indexManager = indexManager;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        indexManager.manageApplicationIndexes(event.getApplicationContext().getClassLoader());
    }

    void applyIndexManagement(BaseValkeyRepository<?> repository) {
        indexManager.applyIndexManagement(repository);
    }
}
