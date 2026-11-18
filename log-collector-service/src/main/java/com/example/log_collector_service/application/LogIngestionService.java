package com.example.log_collector_service.application;

import com.example.log_collector_service.domain.LogEvent;
import com.example.log_collector_service.http.exception.LogPublicationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class LogIngestionService {
    private final LogPublisher logPublisher;
    private final BatchMetrics batchMetrics;

    public CompletableFuture<Void> ingest(List<LogEvent> events) {
        List<CompletableFuture<Void>> publications = new ArrayList<>();
        for (LogEvent event : events) {
            try {
                publications.add(logPublisher.publish(event));
            } catch (RuntimeException exception) {
                publications.add(CompletableFuture.failedFuture(exception));
                break;
            }
        }
        return CompletableFuture.allOf(publications.toArray(CompletableFuture[]::new))
                .handle((ignored, failure) -> {
                    if (failure != null) {
                        batchMetrics.recordFailedBatch();
                        log.atWarn().addKeyValue("batchSize", events.size())
                                .log("Batch publication could not be confirmed");
                        throw new LogPublicationException(failure);
                    }
                    batchMetrics.recordAcceptedBatch();
                    return null;
                });
    }
}
