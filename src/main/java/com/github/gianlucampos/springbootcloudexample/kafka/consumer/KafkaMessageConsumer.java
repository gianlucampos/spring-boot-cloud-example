package com.github.gianlucampos.springbootcloudexample.kafka.consumer;

public interface KafkaMessageConsumer {

    void consume(String message);
}
