package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.EventKeys;
import com.example.log_processor_service.domain.WindowMetrics;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class MetricsSnapshotBuffer extends ContextualProcessor<Windowed<String>, WindowMetrics, String, WindowMetrics> {
    static final String PENDING_METRICS_STORE = "pending-metrics";

    private final Duration flushInterval;
    private KeyValueStore<String, WindowMetrics> pendingMetrics;

    MetricsSnapshotBuffer(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }

    @Override
    public void init(ProcessorContext<String, WindowMetrics> context) {
        super.init(context);
        pendingMetrics = context.getStateStore(PENDING_METRICS_STORE);
        context.schedule(flushInterval, PunctuationType.WALL_CLOCK_TIME, this::flush);
    }

    @Override
    public void process(Record<Windowed<String>, WindowMetrics> record) {
        var metrics = record.value();
        pendingMetrics.put(EventKeys.window(metrics.serviceName(), metrics.windowStart()), metrics);
    }

    private void flush(long timestamp) {
        List<KeyValue<String, WindowMetrics>> snapshots = new ArrayList<>();
        try (var iterator = pendingMetrics.all()) {
            iterator.forEachRemaining(snapshots::add);
        }
        snapshots.forEach(snapshot -> {
            context().forward(new Record<>(snapshot.key, snapshot.value, timestamp));
            pendingMetrics.delete(snapshot.key);
        });
    }
}
