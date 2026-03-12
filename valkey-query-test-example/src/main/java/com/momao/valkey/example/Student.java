package com.momao.valkey.example;

import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.annotation.ValkeyDocument;
import com.momao.valkey.annotation.ValkeyId;
import com.momao.valkey.annotation.ValkeyIndexed;
import com.momao.valkey.annotation.ValkeySearchable;

@ValkeyDocument(value = "student", indexName = "idx:student", prefixes = {"student:"}, storageType = StorageType.HASH)
public class Student {

    @ValkeyId
    private Long id;

    @ValkeySearchable(weight = 1.5d)
    private String name;

    @ValkeyIndexed(sortable = true)
    private Integer age;

    @ValkeyIndexed(sortable = true)
    private Double score;

    @ValkeySearchable("class_name")
    private String className;

    @ValkeyIndexed
    private String department;

    @ValkeyIndexed
    private String status;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
