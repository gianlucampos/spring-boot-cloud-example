package com.github.gianlucampos.springbootcloudexample;

import com.github.gianlucampos.springbootcloudexample.config.KafkaMigrationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaMigrationProperties.class)
public class SpringBootCloudExampleApplication {

    static void main(String[] args) {
        SpringApplication.run(SpringBootCloudExampleApplication.class, args);
    }

}
