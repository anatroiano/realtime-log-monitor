package com.example.log_processor_service.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record LogEvent(
        @NotBlank @Size(max = 128) String logId,
        @NotNull Instant timestamp,
        @NotBlank @Size(max = 128) String serviceName,
        @NotBlank @Size(max = 255) String host,
        @NotNull LogLevel level,
        @NotBlank @Size(max = 8192) String message) {
}
