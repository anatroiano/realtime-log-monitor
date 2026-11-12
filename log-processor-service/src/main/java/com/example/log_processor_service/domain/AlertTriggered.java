package com.example.log_processor_service.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

public record AlertTriggered(String alertId, String serviceName, String ruleId,
                             Instant windowStart, Instant windowEnd, long errorCount) {
    public static final String RULE_ID = "errors-per-minute-over-50";

    public static AlertTriggered from(WindowMetrics metrics) {
        String identity = RULE_ID + ":" + EventKeys.window(metrics.serviceName(), metrics.windowStart());
        String alertId = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
        return new AlertTriggered(alertId, metrics.serviceName(), RULE_ID,
                metrics.windowStart(), metrics.windowEnd(), metrics.errorCount());
    }
}
