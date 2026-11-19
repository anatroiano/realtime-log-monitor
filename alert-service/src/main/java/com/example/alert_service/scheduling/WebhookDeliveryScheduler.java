package com.example.alert_service.scheduling;

import com.example.alert_service.service.WebhookDeliveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@ConditionalOnBooleanProperty("alerts.webhook.enabled")
public class WebhookDeliveryScheduler {
    private final WebhookDeliveryService webhookDeliveryService;

    @Scheduled(fixedDelayString = "${alerts.webhook.poll-interval}")
    public void deliverDue() {
        webhookDeliveryService.deliverDue();
    }
}
