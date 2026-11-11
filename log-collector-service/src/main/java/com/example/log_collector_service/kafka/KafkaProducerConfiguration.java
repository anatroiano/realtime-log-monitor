package com.example.log_collector_service.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.ProducerListener;

@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerConfiguration.class);

    @Bean
    ProducerListener<Object, Object> producerListener() {
        return new ProducerListener<>() {
            @Override
            public void onError(@NonNull ProducerRecord<Object, Object> record, RecordMetadata metadata,
                                @NonNull Exception exception) {
                logger.atWarn()
                        .addKeyValue("topic", record.topic())
                        .addKeyValue("partition", record.partition())
                        .addKeyValue("failureType", exception.getClass().getSimpleName())
                        .log("Kafka publication failed");
            }
        };
    }
}
