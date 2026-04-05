package com.momao.valkey.example;

import com.momao.valkey.annotation.DistanceMetric;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.annotation.ValkeyId;
import com.momao.valkey.annotation.ValkeySearchable;
import com.momao.valkey.annotation.ValkeyTag;
import com.momao.valkey.annotation.ValkeyVector;

@ValkeyDocument(indexName = "idx:vector_item")
public class VectorItem {

    @ValkeyId
    private String id;

    @ValkeySearchable
    private String title;

    @ValkeyTag
    private String category;

    @ValkeyVector(dimension = 3, distanceMetric = DistanceMetric.COSINE)
    private float[] embedding;

    public VectorItem() {
    }

    public VectorItem(String id, String title, String category, float[] embedding) {
        this.id = id;
        this.title = title;
        this.category = category;
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

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public float[] getEmbedding() {
        return embedding;
    }

    public void setEmbedding(float[] embedding) {
        this.embedding = embedding;
    }
}
