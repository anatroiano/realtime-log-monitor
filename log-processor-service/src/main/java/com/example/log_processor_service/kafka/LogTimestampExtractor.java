package com.example.log_processor_service.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

final class LogTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof DecodedLog log && log.isValid()
                && log.event().serviceName().equals(record.key())) {
            return log.event().timestamp().toEpochMilli();
        }
        return Math.max(partitionTime, 0);
    }
}
