package com.example.alert_service.dto;

import com.example.alert_service.event.AlertTriggered;

import java.time.Instant;
import java.util.UUID;

public record AlertNotification(UUID alertId, String serviceName, String ruleId, Instant windowStart,
                                Instant windowEnd, long errorCount) {

    public static AlertNotification from(AlertTriggered alert) {
        return new AlertNotification(alert.alertId(), alert.serviceName(), alert.ruleId(), alert.windowStart(),
                alert.windowEnd(), alert.errorCount());
    }
}
