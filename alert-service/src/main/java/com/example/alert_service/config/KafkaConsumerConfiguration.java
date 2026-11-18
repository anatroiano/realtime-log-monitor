package com.example.alert_service.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.converter.ByteArrayJacksonJsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AlertServiceProperties.class)
public class KafkaConsumerConfiguration {
    static final int MAX_RECORD_RETRIES = 4;
    private static final List<Class<? extends Exception>> INFRASTRUCTURE_FAILURES = List.of(
            TransientDataAccessException.class,
            RecoverableDataAccessException.class,
            DataAccessResourceFailureException.class,
            CannotCreateTransactionException.class);

    @Bean
    RecordMessageConverter recordMessageConverter(JsonMapper jsonMapper) {
        return new ByteArrayJacksonJsonMessageConverter(jsonMapper);
    }

    @Bean
    KafkaListenerConfigurer kafkaListenerValidation(LocalValidatorFactoryBean validator) {
        return registrar -> registrar.setValidator(validator);
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaOperations<?, ?> kafkaOperations,
                                          AlertServiceProperties alertServiceProperties,
                                          MeterRegistry meterRegistry) {
        var errorHandler = new DefaultErrorHandler(
                deadLetterRecoverer(kafkaOperations, alertServiceProperties, meterRegistry), recordBackOff());
        errorHandler.setBackOffFunction((record, exception) ->
                isInfrastructureFailure(exception) ? infrastructureBackOff() : null);
        return errorHandler;
    }

    static boolean isInfrastructureFailure(Throwable exception) {
        return Stream.iterate(exception, Objects::nonNull, Throwable::getCause)
                .anyMatch(cause -> INFRASTRUCTURE_FAILURES.stream().anyMatch(type -> type.isInstance(cause)));
    }

    static BackOff recordBackOff() {
        var backOff = new ExponentialBackOffWithMaxRetries(MAX_RECORD_RETRIES);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5_000);
        return backOff;
    }

    static BackOff infrastructureBackOff() {
        var backOff = new ExponentialBackOff(1_000, 2.0);
        backOff.setMaxInterval(30_000);
        return backOff;
    }

    private ConsumerRecordRecoverer deadLetterRecoverer(KafkaOperations<?, ?> kafkaOperations,
                                                        AlertServiceProperties alertServiceProperties,
                                                        MeterRegistry meterRegistry) {
        var deadLetterPublisher = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, exception) -> new TopicPartition(alertServiceProperties.deadLetterTopic(record.topic()), -1));
        return (record, exception) -> {
            deadLetterPublisher.accept(record, exception);
            meterRegistry.counter("alerts.dead.letters", "topic", record.topic()).increment();
            log.atWarn()
                    .addKeyValue("topic", record.topic())
                    .addKeyValue("partition", record.partition())
                    .addKeyValue("offset", record.offset())
                    .addKeyValue("failureType",
                            NestedExceptionUtils.getMostSpecificCause(exception).getClass().getSimpleName())
                    .log("Record routed to dead-letter topic");
        };
    }
}
