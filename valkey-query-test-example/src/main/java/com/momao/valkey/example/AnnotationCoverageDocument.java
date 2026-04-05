package com.momao.valkey.example;

import com.momao.valkey.annotation.DistanceMetric;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.annotation.ValkeyId;
import com.momao.valkey.annotation.ValkeyIndexed;
import com.momao.valkey.annotation.ValkeyNumeric;
import com.momao.valkey.annotation.ValkeySearchable;
import com.momao.valkey.annotation.ValkeyTag;
import com.momao.valkey.annotation.ValkeyVector;

import java.util.List;

@ValkeyDocument(
        indexName = "idx:annotation_coverage",
        prefixes = {"annotation:"},
        storageType = StorageType.JSON,
        querySuffix = "SearchQuery"
)
public class AnnotationCoverageDocument {

    @ValkeyId
    private String id;

    @ValkeySearchable(value = "title_text", weight = 3.0d, noStem = true, sortable = true)
    private String title;

    @ValkeyTag("status_tag")
    private String status;

    @ValkeyNumeric(value = "play_count", sortable = true)
    private Integer playCount;

    @ValkeyIndexed(sortable = true)
    private Long rank;

    @ValkeySearchable(value = "keyword_text", sortable = true)
    private String keyword;

    @ValkeyTag("labels_tag")
    private List<String> labels;

    @ValkeyVector(
            value = "embedding_vector",
            dimension = 4,
            distanceMetric = DistanceMetric.IP,
            m = 8,
            efConstruction = 32
    )
    private float[] embedding;

    public AnnotationCoverageDocument() {
    }

    public AnnotationCoverageDocument(
            String id,
            String title,
            String status,
            Integer playCount,
            Long rank,
            String keyword,
            List<String> labels,
            float[] embedding) {
        this.id = id;
        this.title = title;
        this.status = status;
        this.playCount = playCount;
        this.rank = rank;
        this.keyword = keyword;
        this.labels = labels;
        this.embedding = embedding;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }

    public Long getRank() {
        return rank;
    }

    public void setRank(Long rank) {
        this.rank = rank;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public List<String> getLabels() {
        return labels;
    }

    public void setLabels(List<String> labels) {
        this.labels = labels;
    }

    public float[] getEmbedding() {
        return embedding;
    }

    public void setEmbedding(float[] embedding) {
        this.embedding = embedding;
    }
}
