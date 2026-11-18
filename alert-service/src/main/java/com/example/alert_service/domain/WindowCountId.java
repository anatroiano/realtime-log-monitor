package com.example.alert_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.time.Instant;

@Embeddable
public record WindowCountId(
        @Column(name = "service_name", nullable = false, length = 128) String serviceName,
        @Column(name = "window_start", nullable = false) Instant windowStart) implements Serializable {
}
