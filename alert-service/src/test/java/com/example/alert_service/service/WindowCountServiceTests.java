package com.example.alert_service.service;

import com.example.alert_service.event.WindowMetrics;
import com.example.alert_service.repository.WindowCountRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

class WindowCountServiceTests {
    private static final Instant NOW = Instant.parse("2026-11-11T12:05:00Z");
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");

    private final WindowCountRepository windowCountRepository = mock(WindowCountRepository.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final WindowCountService windowCountService =
            new WindowCountService(windowCountRepository, Clock.fixed(NOW, ZoneOffset.UTC),
                    new AlertMetrics(meterRegistry));

    @Test
    void storesSnapshotTotalsWithProcessingInstant() {
        given(windowCountRepository.upsertIfNewer(anyString(), any(), any(), anyLong(), anyLong(), any()))
                .willReturn(1);

        windowCountService.record(new WindowMetrics("checkout", START, START.plusSeconds(60), 52L, 51L));

        then(windowCountRepository).should().upsertIfNewer("checkout", START, START.plusSeconds(60), 52L, 51L, NOW);
        assertThat(snapshots("applied")).isEqualTo(1);
        assertThat(snapshots("stale")).isZero();
    }

    @Test
    void countsSnapshotsThatDoNotAdvanceTheWindow() {
        given(windowCountRepository.upsertIfNewer(anyString(), any(), any(), anyLong(), anyLong(), any()))
                .willReturn(0);

        windowCountService.record(new WindowMetrics("checkout", START, START.plusSeconds(60), 10L, 2L));

        assertThat(snapshots("stale")).isEqualTo(1);
        assertThat(snapshots("applied")).isZero();
    }

    private double snapshots(String result) {
        return meterRegistry.get("alerts.window.snapshots").tag("result", result).counter().count();
    }
}
