package com.example.alert_service.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("alerts")
public record AlertServiceProperties(
        @NotBlank String metricsTopic,
        @NotBlank String alertsTopic,
        @NotBlank String deadLetterSuffix,
        @NotNull Duration deadLetterRetention,
        @Min(1) int partitions,
        @Min(1) short replicas
) {
    public String deadLetterTopic(String sourceTopic) {
        return sourceTopic + deadLetterSuffix;
    }
}
