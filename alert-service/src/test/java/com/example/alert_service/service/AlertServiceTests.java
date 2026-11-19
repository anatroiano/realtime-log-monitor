package com.example.alert_service.service;

import com.example.alert_service.domain.Alert;
import com.example.alert_service.event.AlertTriggered;
import com.example.alert_service.repository.AlertRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

class AlertServiceTests {
    private static final Instant NOW = Instant.parse("2026-11-11T12:01:30Z");
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final AlertTriggered ALERT = new AlertTriggered(UUID.fromString(
            "6f1c2a52-6a4e-3d39-9b55-0c9f1b8f7a10"), "checkout", "errors-per-minute-over-50",
            START, START.plusSeconds(60), 51L);

    private final AlertRepository alertRepository = mock(AlertRepository.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final WebhookDeliveryService webhookDeliveryService = mock(WebhookDeliveryService.class);
    private final AlertService alertService =
            new AlertService(alertRepository, Clock.fixed(NOW, ZoneOffset.UTC),
                    new AlertMetrics(meterRegistry), webhookDeliveryService);

    @Test
    void registersNewAlertWithProcessingInstant() {
        given(alertRepository.insertIfAbsent(any(), anyString(), anyString(), any(), any(), anyLong(), any()))
                .willReturn(1);

        alertService.register(ALERT);

        then(alertRepository).should().insertIfAbsent(ALERT.alertId(), "checkout", "errors-per-minute-over-50",
                START, START.plusSeconds(60), 51L, NOW);
        assertThat(incidents("registered")).isEqualTo(1);
        assertThat(incidents("duplicate")).isZero();
        then(webhookDeliveryService).should().schedule(ALERT, NOW);
    }

    @Test
    void ignoresRedeliveredAlert() {
        given(alertRepository.insertIfAbsent(any(), anyString(), anyString(), any(), any(), anyLong(), any()))
                .willReturn(0);

        alertService.register(ALERT);

        assertThat(incidents("duplicate")).isEqualTo(1);
        assertThat(incidents("registered")).isZero();
        then(webhookDeliveryService).should(never()).schedule(any(), any());
    }

    @Test
    void filtersHistoryOnlyWhenServiceIsProvided() {
        var page = PageRequest.of(0, 10);
        Page<Alert> allAlerts = Page.empty(page);
        Page<Alert> checkoutAlerts = Page.empty(page);
        given(alertRepository.findAll(page)).willReturn(allAlerts);
        given(alertRepository.findByServiceName("checkout", page)).willReturn(checkoutAlerts);

        assertThat(alertService.findHistory(null, page)).isSameAs(allAlerts);
        assertThat(alertService.findHistory("checkout", page)).isSameAs(checkoutAlerts);

        then(alertRepository).should().findAll(page);
        then(alertRepository).should().findByServiceName("checkout", page);
    }

    private double incidents(String result) {
        return meterRegistry.get("alerts.incidents").tag("result", result).counter().count();
    }
}
