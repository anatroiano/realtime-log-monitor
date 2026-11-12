package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.LogEvent;

record ProcessedLog(LogEvent event, RejectedLog rejection) {
}
