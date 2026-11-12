package com.example.log_processor_service.kafka;

import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
public class KafkaTopicConfiguration {
    @Bean
    KafkaAdmin.NewTopics processorTopics(ProcessorProperties properties) {
        return new KafkaAdmin.NewTopics(
                topic(properties.rawTopic(), "delete", "CreateTime", properties).build(),
                topic(properties.metricsTopic(), "compact,delete", "CreateTime", properties).build(),
                topic(properties.alertsTopic(), "delete", "CreateTime", properties).build(),
                topic(properties.deadLetterTopic(), "delete", "LogAppendTime", properties)
                        .config(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "2097152").build());
    }

    private TopicBuilder topic(String name, String cleanupPolicy, String timestampType, ProcessorProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.partitions())
                .replicas(properties.replicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(properties.outputRetention().toMillis()))
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timestampType)
                .config(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, Long.toString(Long.MAX_VALUE));
    }
}
