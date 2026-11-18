package com.example.alert_service.dto;

import com.example.alert_service.domain.Alert;
import java.time.Instant;
import java.util.UUID;

public record AlertResponse(UUID alertId, String serviceName, String ruleId, Instant windowStart,
                            Instant windowEnd, long errorCount, Instant registeredAt) {

    public static AlertResponse from(Alert alert) {
        return new AlertResponse(alert.getAlertId(), alert.getServiceName(), alert.getRuleId(),
                alert.getWindowStart(), alert.getWindowEnd(), alert.getErrorCount(), alert.getRegisteredAt());
    }
}
