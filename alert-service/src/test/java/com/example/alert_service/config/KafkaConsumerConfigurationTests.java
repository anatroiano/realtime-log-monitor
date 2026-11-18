package com.example.alert_service.config;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOffExecution;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigurationTests {

    @Test
    void treatsUnavailableDatabaseAsInfrastructureFailure() {
        assertThat(KafkaConsumerConfiguration.isInfrastructureFailure(
                new ListenerExecutionFailedException("listener failed",
                        new CannotCreateTransactionException("connection refused")))).isTrue();
        assertThat(KafkaConsumerConfiguration.isInfrastructureFailure(
                new ListenerExecutionFailedException("listener failed",
                        new QueryTimeoutException("statement timeout")))).isTrue();
    }

    @Test
    void treatsRecordSpecificFailuresAsBoundedRetries() {
        assertThat(KafkaConsumerConfiguration.isInfrastructureFailure(
                new ListenerExecutionFailedException("listener failed",
                        new DataIntegrityViolationException("check constraint")))).isFalse();
        assertThat(KafkaConsumerConfiguration.isInfrastructureFailure(
                new IllegalStateException("unexpected"))).isFalse();
    }

    @Test
    void stopsRetryingARecordAfterTheConfiguredAttempts() {
        BackOffExecution execution = KafkaConsumerConfiguration.recordBackOff().start();

        IntStream.range(0, KafkaConsumerConfiguration.MAX_RECORD_RETRIES)
                .forEach(attempt -> assertThat(execution.nextBackOff()).isNotEqualTo(BackOffExecution.STOP));
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    void keepsRetryingWhileInfrastructureIsUnavailable() {
        BackOffExecution execution = KafkaConsumerConfiguration.infrastructureBackOff().start();

        IntStream.range(0, 1_000)
                .forEach(attempt -> assertThat(execution.nextBackOff()).isBetween(1_000L, 30_000L));
    }
}
