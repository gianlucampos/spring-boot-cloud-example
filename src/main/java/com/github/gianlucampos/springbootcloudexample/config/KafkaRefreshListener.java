package com.github.gianlucampos.springbootcloudexample.config;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaRefreshListener {

    private final KafkaConsumerManager kafkaConsumerManager;

    @EventListener
    public void onRefresh(RefreshScopeRefreshedEvent event) {
        kafkaConsumerManager.refreshConsumers();
    }
}
