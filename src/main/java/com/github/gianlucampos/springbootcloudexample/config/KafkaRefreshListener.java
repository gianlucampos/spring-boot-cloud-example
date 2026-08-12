package com.github.gianlucampos.springbootcloudexample.config;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaRefreshListener {

    private final KafkaListenerManager kafkaListenerManager;

    @EventListener
    public void onRefresh(RefreshScopeRefreshedEvent event) {
        kafkaListenerManager.restartListeners();
    }
}
