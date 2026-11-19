package com.example.alert_service.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

@Validated
@ConfigurationProperties("alerts.webhook")
public record WebhookProperties(
        boolean enabled,
        URI url,
        @NotNull Duration cooldown,
        @NotNull Duration pollInterval,
        @Min(1) int batchSize,
        @Min(1) int maxAttempts,
        @NotNull Duration initialBackoff,
        @NotNull Duration maxBackoff,
        @NotNull Duration connectTimeout,
        @NotNull Duration readTimeout
) {
    @AssertTrue(message = "alerts.webhook.url must be an absolute http or https URL when webhooks are enabled")
    public boolean isUrlValidWhenEnabled() {
        return !enabled || Optional.ofNullable(url)
                .map(URI::getScheme)
                .filter(scheme -> scheme.equals("http") || scheme.equals("https"))
                .isPresent();
    }
}
