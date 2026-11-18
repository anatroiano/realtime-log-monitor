package com.example.log_collector_service.application;

import com.example.log_collector_service.domain.LogEvent;
import com.example.log_collector_service.domain.LogLevel;
import com.example.log_collector_service.http.exception.LogPublicationException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogIngestionServiceTests {
    private final LogEvent first = event("first");
    private final LogEvent second = event("second");

    @Test
    void waitsForEveryBrokerAcknowledgment() {
        var firstSend = new CompletableFuture<Void>();
        var secondSend = new CompletableFuture<Void>();
        var registry = new SimpleMeterRegistry();
        var service = new LogIngestionService(
                event -> event.logId().equals("first") ? firstSend : secondSend, new BatchMetrics(registry));

        var result = service.ingest(List.of(first, second));
        assertThat(result).isNotDone();
        firstSend.complete(null);
        assertThat(result).isNotDone();
        secondSend.complete(null);
        assertThat(result).isCompleted();
        assertThat(registry.get("collector.batches").tag("result", "accepted").counter().count()).isEqualTo(1);
    }

    @Test
    void failsPartiallyPublishedBatchAndPreservesIdsOnRetry() {
        var sent = new ArrayList<LogEvent>();
        var attempt = new AtomicInteger();
        var registry = new SimpleMeterRegistry();
        var service = new LogIngestionService(event -> {
            sent.add(event);
            return attempt.getAndIncrement() == 1
                    ? CompletableFuture.failedFuture(new IllegalStateException("Broker unavailable"))
                    : CompletableFuture.completedFuture(null);
        }, new BatchMetrics(registry));
        var batch = List.of(first, second);

        assertThatThrownBy(() -> service.ingest(batch).join())
                .isInstanceOf(CompletionException.class).hasCauseInstanceOf(LogPublicationException.class);
        service.ingest(batch).join();

        assertThat(sent).containsExactly(first, second, first, second);
        assertThat(registry.get("collector.batches").tag("result", "failed").counter().count()).isEqualTo(1);
    }

    @Test
    void handlesSynchronousFailureAfterAnEarlierSend() {
        var pending = new CompletableFuture<Void>();
        var service = new LogIngestionService(event -> {
            if (event.logId().equals("second")) {
                throw new IllegalStateException("Metadata unavailable");
            }
            return pending;
        }, new BatchMetrics(new SimpleMeterRegistry()));

        var result = service.ingest(List.of(first, second));
        assertThat(result).isNotDone();
        pending.complete(null);
        assertThatThrownBy(result::join).hasCauseInstanceOf(LogPublicationException.class);
    }

    private LogEvent event(String id) {
        return new LogEvent(id, Instant.parse("2026-11-09T20:00:00Z"),
                "checkout", "host-1", LogLevel.ERROR, "Payment failed");
    }
}
