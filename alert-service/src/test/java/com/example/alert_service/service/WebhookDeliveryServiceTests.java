package com.example.alert_service.service;

import com.example.alert_service.client.WebhookClient;
import com.example.alert_service.config.WebhookProperties;
import com.example.alert_service.domain.WebhookDelivery;
import com.example.alert_service.domain.WebhookDeliveryStatus;
import com.example.alert_service.event.AlertTriggered;
import com.example.alert_service.exception.WebhookDeliveryException;
import com.example.alert_service.repository.NotificationCooldownRepository;
import com.example.alert_service.repository.WebhookDeliveryRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.PageRequest;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

class WebhookDeliveryServiceTests {
    private static final Instant NOW = Instant.parse("2026-11-11T12:01:30Z");
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final UUID ALERT_ID = UUID.fromString("6f1c2a52-6a4e-3d39-9b55-0c9f1b8f7a10");
    private static final AlertTriggered ALERT = new AlertTriggered(ALERT_ID, "checkout",
            "errors-per-minute-over-50", START, START.plusSeconds(60), 51L);

    private final WebhookDeliveryRepository webhookDeliveryRepository = mock(WebhookDeliveryRepository.class);
    private final NotificationCooldownRepository notificationCooldownRepository =
            mock(NotificationCooldownRepository.class);
    private final WebhookClient webhookClient = mock(WebhookClient.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    @Test
    void schedulesAPendingDeliveryWithTheAlertPayload() {
        given(notificationCooldownRepository.claimNotification(any(), any(), any())).willReturn(1);

        service(properties(true)).schedule(ALERT, NOW);

        then(notificationCooldownRepository).should()
                .claimNotification("checkout", NOW, NOW.minus(Duration.ofMinutes(5)));
        var captor = ArgumentCaptor.forClass(WebhookDelivery.class);
        then(webhookDeliveryRepository).should().save(captor.capture());
        var delivery = captor.getValue();
        assertThat(delivery.getAlertId()).isEqualTo(ALERT_ID);
        assertThat(delivery.getStatus()).isEqualTo(WebhookDeliveryStatus.PENDING);
        assertThat(delivery.getAttempts()).isZero();
        assertThat(delivery.getNextAttemptAt()).isEqualTo(NOW);
        assertThat(delivery.getPayload())
                .contains("\"alertId\":\"" + ALERT_ID + "\"")
                .contains("\"windowStart\":\"2026-11-11T12:00:00Z\"")
                .contains("\"errorCount\":51");
    }

    @Test
    void suppressesNotificationsInsideTheServiceCooldown() {
        given(notificationCooldownRepository.claimNotification(any(), any(), any())).willReturn(0);

        service(properties(true)).schedule(ALERT, NOW);

        then(webhookDeliveryRepository).should(never()).save(any());
        assertThat(deliveries("suppressed")).isEqualTo(1);
    }

    @Test
    void schedulesNothingWhenWebhooksAreDisabled() {
        service(properties(false)).schedule(ALERT, NOW);

        then(notificationCooldownRepository).shouldHaveNoInteractions();
        then(webhookDeliveryRepository).shouldHaveNoInteractions();
    }

    @Test
    void marksDueDeliveriesAsSent() {
        var delivery = WebhookDelivery.pending(ALERT_ID, "{}", START);
        given(webhookDeliveryRepository.findDueForUpdate(NOW, PageRequest.of(0, 20))).willReturn(List.of(delivery));

        assertThat(service(properties(true)).deliverDue()).isEqualTo(1);

        then(webhookClient).should().send(ALERT_ID, "{}");
        assertThat(delivery.getStatus()).isEqualTo(WebhookDeliveryStatus.SENT);
        assertThat(delivery.getSentAt()).isEqualTo(NOW);
        assertThat(delivery.getAttempts()).isEqualTo(1);
        assertThat(deliveries("sent")).isEqualTo(1);
    }

    @Test
    void retriesTransientFailuresWithExponentialBackoff() {
        var delivery = WebhookDelivery.pending(ALERT_ID, "{}", START);
        given(webhookDeliveryRepository.findDueForUpdate(any(), any())).willReturn(List.of(delivery));
        willThrow(new WebhookDeliveryException("HTTP 503", true, null)).given(webhookClient).send(any(), any());
        var service = service(properties(true));

        service.deliverDue();
        assertThat(delivery.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(5));
        service.deliverDue();
        assertThat(delivery.getNextAttemptAt()).isEqualTo(NOW.plusSeconds(10));

        assertThat(delivery.getStatus()).isEqualTo(WebhookDeliveryStatus.PENDING);
        assertThat(delivery.getAttempts()).isEqualTo(2);
        assertThat(delivery.getLastError()).isEqualTo("HTTP 503");
        assertThat(deliveries("retried")).isEqualTo(2);
    }

    @Test
    void capsTheBackoffAtTheConfiguredMaximum() {
        var delivery = new WebhookDelivery(ALERT_ID, "{}", WebhookDeliveryStatus.PENDING, 6, NOW, START,
                null, "HTTP 503");
        given(webhookDeliveryRepository.findDueForUpdate(any(), any())).willReturn(List.of(delivery));
        willThrow(new WebhookDeliveryException("HTTP 503", true, null)).given(webhookClient).send(any(), any());

        service(properties(true)).deliverDue();

        assertThat(delivery.getNextAttemptAt()).isEqualTo(NOW.plus(Duration.ofMinutes(1)));
    }

    @Test
    void failsPermanentlyOnNonRetryableResponses() {
        var delivery = WebhookDelivery.pending(ALERT_ID, "{}", START);
        given(webhookDeliveryRepository.findDueForUpdate(any(), any())).willReturn(List.of(delivery));
        willThrow(new WebhookDeliveryException("HTTP 400", false, null)).given(webhookClient).send(any(), any());

        service(properties(true)).deliverDue();

        assertThat(delivery.getStatus()).isEqualTo(WebhookDeliveryStatus.FAILED);
        assertThat(delivery.getLastError()).isEqualTo("HTTP 400");
        assertThat(deliveries("failed")).isEqualTo(1);
    }

    @Test
    void failsPermanentlyWhenAttemptsAreExhausted() {
        var delivery = new WebhookDelivery(ALERT_ID, "{}", WebhookDeliveryStatus.PENDING, 7, NOW, START,
                null, "HTTP 503");
        given(webhookDeliveryRepository.findDueForUpdate(any(), any())).willReturn(List.of(delivery));
        willThrow(new WebhookDeliveryException("HTTP 503", true, null)).given(webhookClient).send(any(), any());

        service(properties(true)).deliverDue();

        assertThat(delivery.getStatus()).isEqualTo(WebhookDeliveryStatus.FAILED);
        assertThat(delivery.getAttempts()).isEqualTo(8);
        assertThat(deliveries("retried")).isZero();
    }

    private WebhookDeliveryService service(WebhookProperties webhookProperties) {
        return new WebhookDeliveryService(webhookDeliveryRepository, notificationCooldownRepository, webhookClient,
                webhookProperties, JsonMapper.builder().build(), new AlertMetrics(meterRegistry),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private WebhookProperties properties(boolean enabled) {
        return new WebhookProperties(enabled, URI.create("http://receiver.test/alerts"), Duration.ofMinutes(5),
                Duration.ofSeconds(2), 20, 8, Duration.ofSeconds(5), Duration.ofMinutes(1),
                Duration.ofSeconds(2), Duration.ofSeconds(5));
    }

    private double deliveries(String result) {
        return meterRegistry.get("alerts.webhook.deliveries").tag("result", result).counter().count();
    }
}
