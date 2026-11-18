package com.example.alert_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "alerts")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@EqualsAndHashCode(of = "alertId")
public class Alert {

    @Id
    @Column(name = "alert_id", nullable = false)
    private UUID alertId;

    @Column(name = "service_name", nullable = false, length = 128)
    private String serviceName;

    @Column(name = "rule_id", nullable = false, length = 64)
    private String ruleId;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "error_count", nullable = false)
    private long errorCount;

    @Column(name = "registered_at", nullable = false)
    private Instant registeredAt;
}
