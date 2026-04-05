package com.momao.valkey.example;

import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.annotation.ValkeyId;
import com.momao.valkey.annotation.ValkeyNumeric;
import com.momao.valkey.annotation.ValkeyTag;

@ValkeyDocument(indexName = "idx:media_asset", prefixes = {"media:"}, storageType = StorageType.JSON)
public class MediaAsset {

    @ValkeyId
    private String id;

    @ValkeyTag("producer_mark")
    private String producerMark;

    @ValkeyTag("audit_status")
    private String auditStatus;

    @ValkeyTag("is_public")
    private Boolean isPublic;

    @ValkeyNumeric(value = "play_count", sortable = true)
    private Long playCount;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProducerMark() {
        return producerMark;
    }

    public void setProducerMark(String producerMark) {
        this.producerMark = producerMark;
    }

    public String getAuditStatus() {
        return auditStatus;
    }

    public void setAuditStatus(String auditStatus) {
        this.auditStatus = auditStatus;
    }

    public Boolean getIsPublic() {
        return isPublic;
    }

    public void setIsPublic(Boolean isPublic) {
        this.isPublic = isPublic;
    }

    public Long getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Long playCount) {
        this.playCount = playCount;
    }
}
