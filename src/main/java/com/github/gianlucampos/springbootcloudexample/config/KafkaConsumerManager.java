package com.github.gianlucampos.springbootcloudexample.config;

import com.github.gianlucampos.springbootcloudexample.kafka.consumer.EntregarPacocaSeuZeConsumer;
import com.github.gianlucampos.springbootcloudexample.kafka.consumer.KafkaMessageConsumer;
import com.github.gianlucampos.springbootcloudexample.kafka.consumer.PagarSorveteSeuJoaoConsumer;
import com.github.gianlucampos.springbootcloudexample.kafka.properties.KafkaMigrationProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.context.event.EventListener;
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
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> currentContainers = new ConcurrentHashMap<>();

    private final EntregarPacocaSeuZeConsumer entregarPacocaSeuZeConsumer;
    private final PagarSorveteSeuJoaoConsumer pagarSorveteSeuJoaoConsumer;

    @EventListener(RefreshScopeRefreshedEvent.class)
    public void onRefresh() {
        startAllConsumers();
    }

    public void startAllConsumers() {
        kafkaMigrationProperties.getTopics().keySet().forEach(this::startConsumer);
    }

    public void startConsumer(String topic) {
        validateRestart(topic);

        stopConsumer(topic);

        String environment = kafkaMigrationProperties.getTopics().get(topic);

        ConsumerFactory<String, String> consumerFactory = switch (environment) {
            case "aws" -> awsConsumerFactory;
            case "gcp" -> gcpConsumerFactory;
            default -> throw new IllegalArgumentException("Unknown Kafka environment: " + environment);
        };

        KafkaMessageConsumer consumer = switch (topic) {
            case "entregar.pacoca.seu_ze" -> entregarPacocaSeuZeConsumer;
            case "pagar.sorvete.seu_joao" -> pagarSorveteSeuJoaoConsumer;
            default -> throw new IllegalArgumentException("No consumer configured for topic: " + topic);
        };

        var container = new ConcurrentMessageListenerContainer<>(consumerFactory, new ContainerProperties(topic));
        container.setupMessageListener(
            (MessageListener<String, String>) record -> consumer.consume(record.value())
        );
        currentContainers.put(topic, container);
        container.start();
    }

    private void stopConsumer(String topic) {
        var container = currentContainers.remove(topic);
        if (container != null) container.stop();
    }

    //TODO Melhorar sistema de trava da migração, para que não seja possível migrar de volta para AWS caso o tópico esteja rodando no GCP
    private void validateRestart(String topic) {
        var currentContainer = currentContainers.get(topic);
        if (currentContainer != null && isRunningOnGcp(currentContainer)) {
            throw new IllegalStateException("Topic " + topic + " is already running on GCP and cannot migrate back to AWS");
        }
    }

    private boolean isRunningOnGcp(ConcurrentMessageListenerContainer<String, String> currentContainer) {
        //Check if the current container is using the GCP consumer factory
        return false;
    }
}
