package com.example.log_processor_service.kafka;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("processor")
public record ProcessorProperties(
        @NotBlank String rawTopic,
        @NotBlank String metricsTopic,
        @NotBlank String alertsTopic,
        @NotBlank String deadLetterTopic,
        Duration gracePeriod,
        Duration deduplicationRetention,
        Duration outputRetention,
        Duration metricsFlushInterval,
        @Min(1) int partitions,
        @Min(1) short replicas
) {
    public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    public ProcessorProperties {
        if (gracePeriod == null || gracePeriod.isNegative()) {
            throw new IllegalArgumentException("processor.grace-period must be non-negative");
        }
        if (deduplicationRetention == null || deduplicationRetention.compareTo(WINDOW_SIZE.plus(gracePeriod)) < 0) {
            throw new IllegalArgumentException("processor.deduplication-retention must cover the window and grace period");
        }
        if (outputRetention == null || outputRetention.isNegative() || outputRetention.isZero()) {
            throw new IllegalArgumentException("processor.output-retention must be positive");
        }
        if (metricsFlushInterval == null || metricsFlushInterval.isNegative() || metricsFlushInterval.isZero()) {
            throw new IllegalArgumentException("processor.metrics-flush-interval must be positive");
        }
    }
}
