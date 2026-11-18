package com.example.alert_service.service;

import com.example.alert_service.event.WindowMetrics;
import com.example.alert_service.repository.WindowCountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

@Service
@RequiredArgsConstructor
public class WindowCountService {
    private final WindowCountRepository windowCountRepository;
    private final Clock clock;
    private final AlertMetrics alertMetrics;

    @Transactional
    public void record(WindowMetrics metrics) {
        int updatedRows = windowCountRepository.upsertIfNewer(metrics.serviceName(), metrics.windowStart(),
                metrics.windowEnd(), metrics.totalLogs(), metrics.errorCount(), clock.instant());
        if (updatedRows == 0) {
            alertMetrics.recordStaleSnapshot();
            return;
        }
        alertMetrics.recordAppliedSnapshot();
    }
}
