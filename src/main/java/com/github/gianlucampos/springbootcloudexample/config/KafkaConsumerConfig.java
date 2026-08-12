package com.github.gianlucampos.springbootcloudexample.config;

import com.github.gianlucampos.springbootcloudexample.kafka.properties.KafkaMigrationProperties;
import com.github.gianlucampos.springbootcloudexample.kafka.properties.KafkaProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaMigrationProperties kafkaMigrationProperties;
    private final KafkaProperties kafkaProperties;

    @Bean
    @RefreshScope
    public ConsumerFactory<String, String> consumerFactory() {

        String environment = kafkaMigrationProperties
            .getTopics()
            .get("entregar.pacoca.seu_ze");

        System.out.println(environment);

        KafkaProperties.Connection connection =
            switch (environment) {
                case "aws" -> kafkaProperties.getAws();
                case "gcp" -> kafkaProperties.getGcp();
                default -> throw new IllegalArgumentException("Unknown Kafka environment: " + environment);
            };

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, connection.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
