package com.github.gianlucampos.springbootcloudexample.kafka.consumer;

import org.springframework.stereotype.Component;

@Component
public class PagarSorveteSeuJoaoConsumer implements KafkaMessageConsumer {

    @Override
    public void consume(String message) {
        System.out.println("SORVETE: " + message);
    }
}
