package com.torres.dwpublisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.torres.dwpublisher.mapper")
public class DwPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(DwPublisherApplication.class, args);
    }

}
