package com.example.alert_service.event;

import jakarta.validation.constraints.*;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public record AlertTriggered(
        @NotNull UUID alertId,
        @NotBlank @Size(max = 128) String serviceName,
        @NotBlank @Size(max = 64) String ruleId,
        @NotNull Instant windowStart,
        @NotNull Instant windowEnd,
        @NotNull @Positive Long errorCount) {

    @AssertTrue(message = "windowEnd must be after windowStart")
    public boolean isWindowOrdered() {
        return Optional.ofNullable(windowStart)
                .flatMap(start -> Optional.ofNullable(windowEnd).map(end -> end.isAfter(start)))
                .orElse(true);
    }
}
