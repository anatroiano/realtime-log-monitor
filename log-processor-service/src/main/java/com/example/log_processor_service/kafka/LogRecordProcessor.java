package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.EventKeys;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

final class LogRecordProcessor extends ContextualFixedKeyProcessor<String, DecodedLog, ProcessedLog> {
    static final String SEEN_LOGS_STORE = "seen-logs";
    private static final Logger logger = LoggerFactory.getLogger(LogRecordProcessor.class);

    private final ProcessorProperties properties;
    private final Counter invalid;
    private final Counter late;
    private final Counter duplicates;
    private final Counter accepted;
    private WindowStore<String, Long> seenLogs;

    LogRecordProcessor(ProcessorProperties properties, MeterRegistry registry) {
        this.properties = properties;
        this.invalid = registry.counter("processor.logs", "result", "invalid");
        this.late = registry.counter("processor.logs", "result", "late");
        this.duplicates = registry.counter("processor.logs", "result", "duplicate");
        this.accepted = registry.counter("processor.logs", "result", "accepted");
    }

    @Override
    public void init(FixedKeyProcessorContext<String, ProcessedLog> context) {
        super.init(context);
        seenLogs = context.getStateStore(SEEN_LOGS_STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, DecodedLog> record) {
        DecodedLog log = record.value();
        if (log == null || !log.isValid()) {
            reject(record, log == null ? "MISSING_VALUE" : log.failureReason());
            return;
        }
        if (!log.event().serviceName().equals(record.key())) {
            reject(record, "KEY_MISMATCH");
            return;
        }
        long streamTime = context().currentStreamTimeMs();
        long windowSize = ProcessorProperties.WINDOW_SIZE.toMillis();
        long windowEnd = record.timestamp() - record.timestamp() % windowSize + windowSize;
        if (streamTime - windowEnd >= properties.gracePeriod().toMillis()) {
            late.increment();
            return;
        }
        String key = EventKeys.log(log.event().serviceName(), log.event().logId());
        Instant from = Instant.ofEpochMilli(Math.max(0, streamTime - properties.deduplicationRetention().toMillis()));
        try (var matches = seenLogs.fetch(key, from, Instant.ofEpochMilli(streamTime))) {
            if (matches.hasNext()) {
                duplicates.increment();
                return;
            }
        }
        seenLogs.put(key, streamTime, streamTime);
        accepted.increment();
        context().forward(record.withValue(new ProcessedLog(log.event(), null)));
    }

    private void reject(FixedKeyRecord<String, DecodedLog> record, String reason) {
        var metadata = context().recordMetadata().orElseThrow();
        byte[] value = record.value() == null ? null : record.value().originalValue();
        var rejection = new RejectedLog(metadata.topic(), metadata.partition(), metadata.offset(),
                record.key(), value, reason);
        invalid.increment();
        logger.atWarn().addKeyValue("topic", metadata.topic()).addKeyValue("partition", metadata.partition())
                .addKeyValue("offset", metadata.offset()).addKeyValue("reason", reason)
                .log("Invalid log routed to dead-letter topic");
        context().forward(record.withValue(new ProcessedLog(null, rejection)));
    }
}
