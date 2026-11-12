package com.example.log_processor_service.domain;

import java.time.Instant;

public record WindowMetrics(String serviceName, Instant windowStart, Instant windowEnd,
                            long totalLogs, long errorCount) {
}
