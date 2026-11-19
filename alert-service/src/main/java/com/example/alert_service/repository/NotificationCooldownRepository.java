package com.example.alert_service.repository;

import com.example.alert_service.domain.NotificationCooldown;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface NotificationCooldownRepository extends JpaRepository<NotificationCooldown, String> {

    @Modifying
    @Query(value = """
            INSERT INTO notification_cooldowns (service_name, last_notified_at)
            VALUES (:serviceName, :notifiedAt)
            ON CONFLICT (service_name) DO UPDATE SET last_notified_at = EXCLUDED.last_notified_at
            WHERE notification_cooldowns.last_notified_at <= :cooldownStart
            """, nativeQuery = true)
    int claimNotification(@Param("serviceName") String serviceName,
                          @Param("notifiedAt") Instant notifiedAt,
                          @Param("cooldownStart") Instant cooldownStart);
}
