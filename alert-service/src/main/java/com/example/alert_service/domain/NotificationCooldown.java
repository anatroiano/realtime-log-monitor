package com.example.alert_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "notification_cooldowns")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(of = "serviceName")
public class NotificationCooldown {

    @Id
    @Column(name = "service_name", nullable = false, length = 128)
    private String serviceName;

    @Column(name = "last_notified_at", nullable = false)
    private Instant lastNotifiedAt;
}
