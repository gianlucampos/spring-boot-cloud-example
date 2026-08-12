package com.github.gianlucampos.springbootcloudexample.controller;

import com.github.gianlucampos.springbootcloudexample.kafka.properties.KafkaMigrationProperties;
import com.github.gianlucampos.springbootcloudexample.kafka.properties.KafkaProperties;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("app/v1/")
@AllArgsConstructor
public class ConfigController {

    private final KafkaMigrationProperties kafkaMigrationProperties;
    private final KafkaProperties kafkaProperties;

    @GetMapping("/topics")
    public Map<String, String> topics() {
        return kafkaMigrationProperties.getTopics();
    }

    @GetMapping("/kafka")
    public KafkaProperties kafka() {
        return kafkaProperties;
    }
}
