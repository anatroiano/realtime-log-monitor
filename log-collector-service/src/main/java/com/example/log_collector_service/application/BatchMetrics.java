package com.example.log_collector_service.application;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class BatchMetrics {
    private final Counter acceptedBatches;
    private final Counter failedBatches;

    public BatchMetrics(MeterRegistry meterRegistry) {
        this.acceptedBatches = meterRegistry.counter("collector.batches", "result", "accepted");
        this.failedBatches = meterRegistry.counter("collector.batches", "result", "failed");
    }

    public void recordAcceptedBatch() {
        acceptedBatches.increment();
    }

    public void recordFailedBatch() {
        failedBatches.increment();
    }
}
