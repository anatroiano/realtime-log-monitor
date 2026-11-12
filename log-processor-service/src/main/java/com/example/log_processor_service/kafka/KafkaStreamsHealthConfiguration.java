package com.example.log_processor_service.kafka;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@Configuration(proxyBeanMethods = false)
public class KafkaStreamsHealthConfiguration {
    @Bean
    HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean factory) {
        return () -> {
            var streams = factory.getKafkaStreams();
            if (streams == null) {
                return Health.down().withDetail("state", "NOT_STARTED").build();
            }
            var state = streams.state();
            var health = state.isRunningOrRebalancing() ? Health.up() : Health.down();
            return health.withDetail("state", state.name()).build();
        };
    }
}
