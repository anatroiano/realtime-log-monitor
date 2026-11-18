package com.example.alert_service.consumer;

import com.example.alert_service.event.AlertTriggered;
import com.example.alert_service.event.WindowMetrics;
import com.example.alert_service.service.AlertService;
import com.example.alert_service.service.WindowCountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AlertEventsConsumer {
    private final WindowCountService windowCountService;
    private final AlertService alertService;

    @KafkaListener(topics = "${alerts.metrics-topic}")
    public void onWindowMetrics(@Payload @Valid WindowMetrics metrics) {
        windowCountService.record(metrics);
    }

    @KafkaListener(topics = "${alerts.alerts-topic}")
    public void onAlertTriggered(@Payload @Valid AlertTriggered alert) {
        alertService.register(alert);
    }
}
