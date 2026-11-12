package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.LogEvent;

record DecodedLog(LogEvent event, byte[] originalValue, String failureReason) {
    boolean isValid() {
        return failureReason == null && event != null;
    }
}
