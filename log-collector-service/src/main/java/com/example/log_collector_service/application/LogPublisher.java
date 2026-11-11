package com.example.log_collector_service.application;

import java.util.concurrent.CompletableFuture;

import com.example.log_collector_service.domain.LogEvent;

public interface LogPublisher {
    CompletableFuture<Void> publish(LogEvent event);
}
