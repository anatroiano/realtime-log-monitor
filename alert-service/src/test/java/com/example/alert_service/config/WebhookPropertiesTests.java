package com.example.alert_service.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class WebhookPropertiesTests {

    @ParameterizedTest
    @CsvSource(nullValues = "null", value = {
            "false, null, true",
            "true, http://webhook-receiver:8080/alerts, true",
            "true, https://hooks.example.com/alerts, true",
            "true, null, false",
            "true, ftp://hooks.example.com/alerts, false",
            "true, /alerts, false"})
    void requiresAnHttpUrlOnlyWhenEnabled(boolean enabled, String url, boolean valid) {
        var webhookProperties = new WebhookProperties(enabled, Optional.ofNullable(url).map(URI::create).orElse(null),
                Duration.ofMinutes(5), Duration.ofSeconds(2), 20, 8, Duration.ofSeconds(5), Duration.ofMinutes(10),
                Duration.ofSeconds(2), Duration.ofSeconds(5));

        assertThat(webhookProperties.isUrlValidWhenEnabled()).isEqualTo(valid);
    }
}
