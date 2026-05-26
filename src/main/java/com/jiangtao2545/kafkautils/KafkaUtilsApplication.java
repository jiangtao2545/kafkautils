package com.jiangtao2545.kafkautils;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.jiangtao2545.kafkautils.config.KafkaClusterProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaClusterProperties.class)
public class KafkaUtilsApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaUtilsApplication.class, args);
    }
}
