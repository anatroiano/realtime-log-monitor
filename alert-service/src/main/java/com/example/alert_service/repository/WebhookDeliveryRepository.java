package com.example.alert_service.repository;

import com.example.alert_service.domain.WebhookDelivery;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface WebhookDeliveryRepository extends JpaRepository<WebhookDelivery, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("""
            SELECT d FROM WebhookDelivery d
            WHERE d.status = 'PENDING' AND d.nextAttemptAt <= :now
            ORDER BY d.nextAttemptAt ASC
            """)
    List<WebhookDelivery> findDueForUpdate(@Param("now") Instant now, Pageable pageable);
}
