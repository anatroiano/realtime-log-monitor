package com.example.log_collector_service.application;

import com.example.log_collector_service.domain.LogEvent;
import com.example.log_collector_service.http.exception.LogPublicationException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class LogIngestionService {
    private static final Logger logger = LoggerFactory.getLogger(LogIngestionService.class);

    private final LogPublisher publisher;
    private final Counter acceptedBatches;
    private final Counter failedBatches;

    public LogIngestionService(LogPublisher publisher, MeterRegistry registry) {
        this.publisher = publisher;
        this.acceptedBatches = registry.counter("collector.batches", "result", "accepted");
        this.failedBatches = registry.counter("collector.batches", "result", "failed");
    }

    public CompletableFuture<Void> ingest(List<LogEvent> events) {
        List<CompletableFuture<Void>> publications = new ArrayList<>();
        for (LogEvent event : events) {
            try {
                publications.add(publisher.publish(event));
            } catch (RuntimeException exception) {
                publications.add(CompletableFuture.failedFuture(exception));
                break;
            }
        }
        return CompletableFuture.allOf(publications.toArray(CompletableFuture[]::new))
                .handle((ignored, failure) -> {
                    if (failure != null) {
                        failedBatches.increment();
                        logger.atWarn().addKeyValue("batchSize", events.size())
                                .log("Batch publication could not be confirmed");
                        throw new LogPublicationException(failure);
                    }
                    acceptedBatches.increment();
                    return null;
                });
    }
}
