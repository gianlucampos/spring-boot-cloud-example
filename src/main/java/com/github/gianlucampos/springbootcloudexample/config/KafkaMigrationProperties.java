package com.github.gianlucampos.springbootcloudexample.config;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
@RefreshScope
@ConfigurationProperties(prefix = "kafka-migration")
public class KafkaMigrationProperties {

    private Map<String, String> topics;

}
