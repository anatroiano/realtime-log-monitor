package com.example.alert_service.repository;

import com.example.alert_service.domain.Alert;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface AlertRepository extends JpaRepository<Alert, UUID> {

    Page<Alert> findByServiceName(String serviceName, Pageable pageable);

    @Modifying
    @Query(value = """
            INSERT INTO alerts (alert_id, service_name, rule_id, window_start, window_end, error_count, registered_at)
            VALUES (:alertId, :serviceName, :ruleId, :windowStart, :windowEnd, :errorCount, :registeredAt)
            ON CONFLICT (alert_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("alertId") UUID alertId,
                       @Param("serviceName") String serviceName,
                       @Param("ruleId") String ruleId,
                       @Param("windowStart") Instant windowStart,
                       @Param("windowEnd") Instant windowEnd,
                       @Param("errorCount") long errorCount,
                       @Param("registeredAt") Instant registeredAt);
}
