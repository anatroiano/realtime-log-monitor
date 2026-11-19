package com.example.alert_service.service;

import com.example.alert_service.client.WebhookClient;
import com.example.alert_service.config.WebhookProperties;
import com.example.alert_service.domain.WebhookDelivery;
import com.example.alert_service.dto.AlertNotification;
import com.example.alert_service.event.AlertTriggered;
import com.example.alert_service.exception.WebhookDeliveryException;
import com.example.alert_service.repository.NotificationCooldownRepository;
import com.example.alert_service.repository.WebhookDeliveryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class WebhookDeliveryService {
    private static final int MAX_BACKOFF_EXPONENT = 20;

    private final WebhookDeliveryRepository webhookDeliveryRepository;
    private final NotificationCooldownRepository notificationCooldownRepository;
    private final WebhookClient webhookClient;
    private final WebhookProperties webhookProperties;
    private final JsonMapper jsonMapper;
    private final AlertMetrics alertMetrics;
    private final Clock clock;

    @Transactional(propagation = Propagation.MANDATORY)
    public void schedule(AlertTriggered alert, Instant registeredAt) {
        if (!webhookProperties.enabled()) {
            return;
        }
        var logEvent = log.atInfo()
                .addKeyValue("alertId", alert.alertId())
                .addKeyValue("serviceName", alert.serviceName());
        int claimedRows = notificationCooldownRepository.claimNotification(alert.serviceName(), registeredAt,
                registeredAt.minus(webhookProperties.cooldown()));
        if (claimedRows == 0) {
            alertMetrics.recordSuppressedNotification();
            logEvent.log("Webhook notification suppressed by cooldown");
            return;
        }
        var payload = jsonMapper.writeValueAsString(AlertNotification.from(alert));
        webhookDeliveryRepository.save(WebhookDelivery.pending(alert.alertId(), payload, registeredAt));
        logEvent.log("Webhook notification scheduled");
    }

    @Transactional
    public int deliverDue() {
        var now = clock.instant();
        var dueDeliveries = webhookDeliveryRepository.findDueForUpdate(now,
                PageRequest.of(0, webhookProperties.batchSize()));
        dueDeliveries.forEach(delivery -> deliver(delivery, now));
        return dueDeliveries.size();
    }

    private void deliver(WebhookDelivery delivery, Instant now) {
        try {
            webhookClient.send(delivery.getAlertId(), delivery.getPayload());
            delivery.markSent(now);
            alertMetrics.recordSentNotification();
            log.atInfo().addKeyValue("alertId", delivery.getAlertId())
                    .addKeyValue("attempts", delivery.getAttempts())
                    .log("Webhook notification sent");
        } catch (WebhookDeliveryException exception) {
            registerFailure(delivery, exception, now);
        }
    }

    private void registerFailure(WebhookDelivery delivery, WebhookDeliveryException exception, Instant now) {
        boolean attemptsExhausted = delivery.getAttempts() + 1 >= webhookProperties.maxAttempts();
        var logEvent = log.atWarn().addKeyValue("alertId", delivery.getAlertId())
                .addKeyValue("attempt", delivery.getAttempts() + 1)
                .addKeyValue("reason", exception.getMessage());
        if (!exception.isRetryable() || attemptsExhausted) {
            delivery.markFailed(exception.getMessage());
            alertMetrics.recordFailedNotification();
            logEvent.log("Webhook notification failed permanently");
            return;
        }
        delivery.scheduleRetry(exception.getMessage(), now.plus(backoff(delivery.getAttempts())));
        alertMetrics.recordRetriedNotification();
        logEvent.addKeyValue("nextAttemptAt", delivery.getNextAttemptAt())
                .log("Webhook notification will be retried");
    }

    private Duration backoff(int previousAttempts) {
        var exponentialBackoff = webhookProperties.initialBackoff()
                .multipliedBy(1L << Math.min(previousAttempts, MAX_BACKOFF_EXPONENT));
        return exponentialBackoff.compareTo(webhookProperties.maxBackoff()) > 0
                ? webhookProperties.maxBackoff()
                : exponentialBackoff;
    }
}
