package com.example.log_processor_service.kafka;

public record RejectedLog(String topic, int partition, long offset, String key, byte[] value, String reason) {
}
