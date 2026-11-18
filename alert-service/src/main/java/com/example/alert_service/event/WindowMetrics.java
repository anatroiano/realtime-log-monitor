package com.example.alert_service.event;

import jakarta.validation.constraints.*;

import java.time.Instant;
import java.util.Optional;

public record WindowMetrics(
        @NotBlank @Size(max = 128) String serviceName,
        @NotNull Instant windowStart,
        @NotNull Instant windowEnd,
        @NotNull @PositiveOrZero Long totalLogs,
        @NotNull @PositiveOrZero Long errorCount) {

    @AssertTrue(message = "windowEnd must be after windowStart")
    public boolean isWindowOrdered() {
        return Optional.ofNullable(windowStart)
                .flatMap(start -> Optional.ofNullable(windowEnd).map(end -> end.isAfter(start)))
                .orElse(true);
    }

    @AssertTrue(message = "errorCount must not exceed totalLogs")
    public boolean isErrorCountWithinTotal() {
        return Optional.ofNullable(totalLogs)
                .flatMap(total -> Optional.ofNullable(errorCount).map(errors -> errors <= total))
                .orElse(true);
    }
}
