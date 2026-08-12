package com.github.gianlucampos.springbootcloudexample.kafka;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
public class EntregarPacocaSeuZeConsumer {

    @KafkaListener(
        topics = "entregar.pacoca.seu_ze",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(String message) {
        System.out.println("MESSAGE: " + message);
    }
}
