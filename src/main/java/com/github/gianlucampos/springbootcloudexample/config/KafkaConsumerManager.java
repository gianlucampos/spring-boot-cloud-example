package com.github.gianlucampos.springbootcloudexample.config;

import com.github.gianlucampos.springbootcloudexample.kafka.consumer.EntregarPacocaSeuZeConsumer;
import com.github.gianlucampos.springbootcloudexample.kafka.consumer.KafkaMessageConsumer;
import com.github.gianlucampos.springbootcloudexample.kafka.consumer.PagarSorveteSeuJoaoConsumer;
import com.github.gianlucampos.springbootcloudexample.kafka.properties.KafkaMigrationProperties;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumerManager {

    private final KafkaMigrationProperties kafkaMigrationProperties;
    private final ConsumerFactory<String, String> awsConsumerFactory;
    private final ConsumerFactory<String, String> gcpConsumerFactory;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> runningContainers = new ConcurrentHashMap<>();
    private final Map<String, String> runningEnvironments = new ConcurrentHashMap<>();

    private final EntregarPacocaSeuZeConsumer entregarPacocaSeuZeConsumer;
    private final PagarSorveteSeuJoaoConsumer pagarSorveteSeuJoaoConsumer;

    public void refreshConsumers() {
        kafkaMigrationProperties.getTopics().forEach((topic, newEnvironment) -> {
            var currentEnvironment = runningEnvironments.get(topic);

            // Consumer ainda não está rodando neste pod
            if (currentEnvironment == null) {
                startConsumer(topic, newEnvironment);
                return;
            }

            // Nada mudou
            if (currentEnvironment.equals(newEnvironment)) {
                return;
            }

            // Ambiente mudou: reinicia somente este consumer
            stopConsumer(topic);
            startConsumer(topic, newEnvironment);
        });
    }

    @PostConstruct
    private void init() {
        kafkaMigrationProperties.getTopics().forEach(this::startConsumer);
    }

    private void startConsumer(String topic, String environment) {
        ConsumerFactory<String, String> consumerFactory = getConsumerFactory(environment);
        var container = createContainer(topic, consumerFactory);
        runningContainers.put(topic, container);
        runningEnvironments.put(topic, environment);
        container.start();
    }

    private void stopConsumer(String topic) {
        var container = runningContainers.remove(topic);
        if (container != null) {
            container.stop();
        }
        runningEnvironments.remove(topic);
    }

    private ConcurrentMessageListenerContainer<String, String> createContainer(String topic, ConsumerFactory<String, String> consumerFactory) {
        var container = new ConcurrentMessageListenerContainer<>(consumerFactory, new ContainerProperties(topic));
        var consumer = getConsumer(topic);
        container.setupMessageListener(
            (MessageListener<String, String>) record -> consumer.consume(record.value())
        );
        return container;
    }

    private ConsumerFactory<String, String> getConsumerFactory(String environment) {
        return switch (environment) {
            case "aws" -> awsConsumerFactory;
            case "gcp" -> gcpConsumerFactory;
            default -> throw new IllegalArgumentException("Unknown Kafka environment: " + environment);
        };
    }

    private KafkaMessageConsumer getConsumer(String topic) {
        return switch (topic) {
            case "entregar.pacoca.seu_ze" -> entregarPacocaSeuZeConsumer;
            case "pagar.sorvete.seu_joao" -> pagarSorveteSeuJoaoConsumer;
            default -> throw new IllegalArgumentException("No consumer configured for topic: " + topic);
        };
    }
}
