package com.momao.valkey.example;

import com.momao.valkey.annotation.ValkeyIndexed;

public class Merchant {

    @ValkeyIndexed
    private String name;

    private String level;

    public Merchant() {
    }

    public Merchant(String name, String level) {
        this.name = name;
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    @Override
    public String toString() {
        return "Merchant{name='%s', level='%s'}".formatted(name, level);
    }
}
