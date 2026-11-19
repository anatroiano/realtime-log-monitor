package com.example.alert_service.client;

import com.example.alert_service.config.WebhookProperties;
import com.example.alert_service.exception.WebhookDeliveryException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withException;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

class WebhookClientTests {
    private static final URI URL = URI.create("http://receiver.test/alerts");
    private static final UUID ALERT_ID = UUID.fromString("6f1c2a52-6a4e-3d39-9b55-0c9f1b8f7a10");
    private static final String PAYLOAD = "{\"alertId\":\"" + ALERT_ID + "\"}";

    private MockRestServiceServer server;
    private WebhookClient webhookClient;

    @BeforeEach
    void setUp() {
        var builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        var webhookProperties = new WebhookProperties(true, URL, Duration.ofMinutes(5), Duration.ofSeconds(2), 20, 8,
                Duration.ofSeconds(5), Duration.ofMinutes(10), Duration.ofSeconds(2), Duration.ofSeconds(5));
        webhookClient = new WebhookClient(builder.build(), webhookProperties);
    }

    @Test
    void postsThePayloadWithTheAlertIdAsIdempotencyKey() {
        server.expect(requestTo(URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header("Idempotency-Key", ALERT_ID.toString()))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json(PAYLOAD))
                .andRespond(withStatus(HttpStatus.ACCEPTED));

        webhookClient.send(ALERT_ID, PAYLOAD);

        server.verify();
    }

    @ParameterizedTest
    @CsvSource({"500, true", "503, true", "408, true", "429, true", "400, false", "404, false", "302, false"})
    void classifiesUnsuccessfulResponses(int status, boolean retryable) {
        server.expect(requestTo(URL)).andRespond(withStatus(HttpStatus.valueOf(status)));

        assertThatThrownBy(() -> webhookClient.send(ALERT_ID, PAYLOAD))
                .isInstanceOfSatisfying(WebhookDeliveryException.class, exception -> {
                    assertThat(exception.getMessage()).isEqualTo("HTTP " + status);
                    assertThat(exception.isRetryable()).isEqualTo(retryable);
                });
    }

    @Test
    void treatsConnectionFailuresAsRetryable() {
        server.expect(requestTo(URL)).andRespond(withException(new IOException("connection refused")));

        assertThatThrownBy(() -> webhookClient.send(ALERT_ID, PAYLOAD))
                .isInstanceOfSatisfying(WebhookDeliveryException.class, exception ->
                        assertThat(exception.isRetryable()).isTrue());
    }
}
