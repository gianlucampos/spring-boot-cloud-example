package com.github.gianlucampos.springbootcloudexample.kafka.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

    private Connection aws;
    private Connection gcp;

    @Setter
    @Getter
    public static class Connection {

        private String bootstrapServers;
        private String groupId;

    }
}
