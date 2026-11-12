package com.example.log_processor_service.domain;

public record WindowCounts(long totalLogs, long errorCount, boolean thresholdCrossed) {
    public static final long ALERT_THRESHOLD = 51;

    public static WindowCounts empty() {
        return new WindowCounts(0, 0, false);
    }

    public WindowCounts add(LogEvent event) {
        long nextErrorCount = errorCount + (event.level().isError() ? 1 : 0);
        return new WindowCounts(totalLogs + 1, nextErrorCount,
                errorCount < ALERT_THRESHOLD && nextErrorCount >= ALERT_THRESHOLD);
    }
}
