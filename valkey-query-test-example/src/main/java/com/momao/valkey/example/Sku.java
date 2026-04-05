package com.momao.valkey.example;

import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.annotation.ValkeyId;
import com.momao.valkey.annotation.ValkeyIndexed;
import com.momao.valkey.annotation.ValkeyNumeric;
import com.momao.valkey.annotation.ValkeySearchable;
import com.momao.valkey.annotation.ValkeyTag;

import java.util.List;

@ValkeyDocument
public class Sku {

    @ValkeyId
    private String id;

    @ValkeySearchable(weight = 2.5d, noStem = true)
    private String title;

    @ValkeyNumeric(sortable = true)
    private Integer price;

    @ValkeyTag
    private List<String> tags;

    @ValkeyIndexed
    private Merchant merchant;

    public Sku() {
    }

    public Sku(String id, String title, Integer price, List<String> tags, Merchant merchant) {
        this.id = id;
        this.title = title;
        this.price = price;
        this.tags = tags;
        this.merchant = merchant;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Merchant getMerchant() {
        return merchant;
    }

    public void setMerchant(Merchant merchant) {
        this.merchant = merchant;
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

    @Override
    public String toString() {
        return "Sku{id='%s', title='%s', price=%s, tags=%s, merchant=%s}".formatted(id, title, price, tags, merchant);
    }
}
