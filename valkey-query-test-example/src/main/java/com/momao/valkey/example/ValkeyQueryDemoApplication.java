package com.momao.valkey.example;

import com.momao.valkey.autoconfigure.EnableValkeyQuery;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableValkeyQuery(basePackages = "com.momao.valkey.example")
public class ValkeyQueryDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(ValkeyQueryDemoApplication.class, args);
    }
}
