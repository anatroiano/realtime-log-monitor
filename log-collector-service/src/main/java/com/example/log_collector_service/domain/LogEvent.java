package com.example.log_collector_service.domain;

import java.time.Instant;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Schema(description = "An application log event", additionalProperties = Schema.AdditionalPropertiesValue.FALSE)
public record LogEvent(
        @Schema(description = "Unique log identifier, preserved when retrying a batch", example = "checkout-001",
                requiredMode = Schema.RequiredMode.REQUIRED)
        @NotBlank @Size(max = 128) String logId,

        @Schema(description = "Original event timestamp with a time zone, normalized to UTC when published",
                example = "2026-11-11T14:30:00Z", requiredMode = Schema.RequiredMode.REQUIRED)
        @NotNull Instant timestamp,

        @Schema(description = "Name of the application that produced the log", example = "checkout",
                requiredMode = Schema.RequiredMode.REQUIRED)
        @NotBlank @Size(max = 128) String serviceName,

        @Schema(description = "Host that produced the log", example = "host-1",
                requiredMode = Schema.RequiredMode.REQUIRED)
        @NotBlank @Size(max = 255) String host,

        @Schema(example = "ERROR", requiredMode = Schema.RequiredMode.REQUIRED)
        @NotNull LogLevel level,

        @Schema(description = "Log message", example = "Payment failed", requiredMode = Schema.RequiredMode.REQUIRED)
        @NotBlank @Size(max = 8192) String message) {
}
