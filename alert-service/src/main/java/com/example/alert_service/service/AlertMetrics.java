package com.example.alert_service.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class AlertMetrics {
    private final Counter appliedSnapshots;
    private final Counter staleSnapshots;
    private final Counter registeredAlerts;
    private final Counter duplicateAlerts;

    public AlertMetrics(MeterRegistry meterRegistry) {
        this.appliedSnapshots = meterRegistry.counter("alerts.window.snapshots", "result", "applied");
        this.staleSnapshots = meterRegistry.counter("alerts.window.snapshots", "result", "stale");
        this.registeredAlerts = meterRegistry.counter("alerts.incidents", "result", "registered");
        this.duplicateAlerts = meterRegistry.counter("alerts.incidents", "result", "duplicate");
    }

    public void recordAppliedSnapshot() {
        appliedSnapshots.increment();
    }

    public void recordStaleSnapshot() {
        staleSnapshots.increment();
    }

    public void recordRegisteredAlert() {
        registeredAlerts.increment();
    }

    public void recordDuplicateAlert() {
        duplicateAlerts.increment();
    }
}
