package com.example.log_collector_service.kafka;

import com.example.log_collector_service.application.LogPublisher;
import com.example.log_collector_service.domain.LogEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaLogPublisher implements LogPublisher {
    private final KafkaTemplate<String, LogEvent> template;
    private final String topic;

    public KafkaLogPublisher(KafkaTemplate<String, LogEvent> template,
                             @Value("${collector.raw-topic}") String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> publish(LogEvent event) {
        return template.send(topic, null, event.timestamp().toEpochMilli(), event.serviceName(), event)
                .thenApply(result -> null);
    }
}
