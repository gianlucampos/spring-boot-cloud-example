package com.github.gianlucampos.springbootcloudexample.config;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaListenerManager {

    private final KafkaListenerEndpointRegistry registry;

    public void restartListeners() {
        registry.getListenerContainers().forEach(container -> {
            container.stop();
            container.start();
        });
    }
}
