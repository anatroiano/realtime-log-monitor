package com.example.alert_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
public class KafkaTopicConfiguration {
    @Bean
    KafkaAdmin.NewTopics deadLetterTopics(AlertServiceProperties alertServiceProperties) {
        return new KafkaAdmin.NewTopics(
                deadLetterTopic(alertServiceProperties.deadLetterTopic(alertServiceProperties.metricsTopic()),
                        alertServiceProperties),
                deadLetterTopic(alertServiceProperties.deadLetterTopic(alertServiceProperties.alertsTopic()),
                        alertServiceProperties));
    }

    private NewTopic deadLetterTopic(String name, AlertServiceProperties alertServiceProperties) {
        return TopicBuilder.name(name)
                .partitions(alertServiceProperties.partitions())
                .replicas(alertServiceProperties.replicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(alertServiceProperties.deadLetterRetention().toMillis()))
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
                .build();
    }
}
