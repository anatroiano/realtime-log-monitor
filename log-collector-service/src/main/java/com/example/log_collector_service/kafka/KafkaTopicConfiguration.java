package com.example.log_collector_service.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

@Configuration(proxyBeanMethods = false)
public class KafkaTopicConfiguration {
    @Bean
    NewTopic rawLogsTopic(@Value("${collector.raw-topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(Duration.ofDays(7).toMillis()))
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "CreateTime")
                .config(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, Long.toString(Long.MAX_VALUE))
                .build();
    }
}
