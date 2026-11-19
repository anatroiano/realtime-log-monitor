package com.example.alert_service.service;

import com.example.alert_service.domain.Alert;
import com.example.alert_service.event.AlertTriggered;
import com.example.alert_service.repository.AlertRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertService {
    private final AlertRepository alertRepository;
    private final Clock clock;
    private final AlertMetrics alertMetrics;
    private final WebhookDeliveryService webhookDeliveryService;

    @Transactional
    public void register(AlertTriggered alert) {
        var registeredAt = clock.instant();
        int insertedRows = alertRepository.insertIfAbsent(alert.alertId(), alert.serviceName(), alert.ruleId(),
                alert.windowStart(), alert.windowEnd(), alert.errorCount(), registeredAt);
        var logEvent = log.atInfo()
                .addKeyValue("alertId", alert.alertId())
                .addKeyValue("serviceName", alert.serviceName())
                .addKeyValue("ruleId", alert.ruleId())
                .addKeyValue("windowStart", alert.windowStart());
        if (insertedRows == 0) {
            alertMetrics.recordDuplicateAlert();
            logEvent.log("Redelivered alert ignored");
            return;
        }
        alertMetrics.recordRegisteredAlert();
        logEvent.log("Alert registered");
        webhookDeliveryService.schedule(alert, registeredAt);
    }

    @Transactional(readOnly = true)
    public Page<Alert> findHistory(String serviceName, Pageable pageable) {
        return Optional.ofNullable(serviceName)
                .map(name -> alertRepository.findByServiceName(name, pageable))
                .orElseGet(() -> alertRepository.findAll(pageable));
    }
}
