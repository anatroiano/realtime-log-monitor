package com.example.alert_service.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "webhook_deliveries")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@EqualsAndHashCode(of = "alertId")
public class WebhookDelivery {

    @Id
    @Column(name = "alert_id", nullable = false)
    private UUID alertId;

    @Column(name = "payload", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private WebhookDeliveryStatus status;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "sent_at")
    private Instant sentAt;

    @Column(name = "last_error")
    private String lastError;

    public static WebhookDelivery pending(UUID alertId, String payload, Instant createdAt) {
        return new WebhookDelivery(alertId, payload, WebhookDeliveryStatus.PENDING, 0, createdAt, createdAt,
                null, null);
    }

    public void markSent(Instant sentAt) {
        this.attempts++;
        this.status = WebhookDeliveryStatus.SENT;
        this.sentAt = sentAt;
        this.lastError = null;
    }

    public void scheduleRetry(String error, Instant nextAttemptAt) {
        this.attempts++;
        this.lastError = error;
        this.nextAttemptAt = nextAttemptAt;
    }

    public void markFailed(String error) {
        this.attempts++;
        this.status = WebhookDeliveryStatus.FAILED;
        this.lastError = error;
    }
}
