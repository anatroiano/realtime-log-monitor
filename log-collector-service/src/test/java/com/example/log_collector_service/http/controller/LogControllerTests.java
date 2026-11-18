package com.example.log_collector_service.http.controller;

import com.example.log_collector_service.application.BatchMetrics;
import com.example.log_collector_service.application.LogIngestionService;
import com.example.log_collector_service.application.LogPublisher;
import com.example.log_collector_service.domain.LogEvent;
import com.example.log_collector_service.domain.LogLevel;
import com.example.log_collector_service.http.contrroller.LogController;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(LogController.class)
@Import({LogIngestionService.class, BatchMetrics.class, LogControllerTests.Configuration.class})
class LogControllerTests {
    private static final LogEvent EVENT = new LogEvent(
            "id-1",
            Instant.parse("2026-11-09T20:00:00Z"),
            "checkout",
            "host-1",
            LogLevel.ERROR,
            "private test message"
    );

    @Autowired
    MockMvc mvc;
    @Autowired
    TestPublisher publisher;
    @Autowired
    JsonMapper mapper;

    @BeforeEach
    void resetPublisher() {
        publisher.events.clear();
        publisher.confirmation = new CompletableFuture<>();
    }

    @Test
    void returnsAcceptedOnlyAfterPublicationCompletes() throws Exception {
        var result = mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(List.of(EVENT))))
                .andExpect(request().asyncStarted()).andReturn();

        assertThat(result.getRequest().isAsyncStarted()).isTrue();
        assertThat(publisher.events).hasSize(1);
        assertThat(publisher.events.getFirst().timestamp().toString()).isEqualTo("2026-11-09T20:00:00Z");
        publisher.confirmation.complete(null);
        mvc.perform(asyncDispatch(result)).andExpect(status().isAccepted());
    }

    @Test
    void returnsRetryInstructionsWhenPublicationFails() throws Exception {
        publisher.confirmation.completeExceptionally(new IllegalStateException("private test message"));
        var result = mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(List.of(EVENT))))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(result)).andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.detail").value(
                        "Batch publication could not be confirmed. Retry the entire batch with the same logId values."));
    }

    @ParameterizedTest
    @MethodSource("invalidBatchBodies")
    void rejectsInvalidBatchWithoutPublishing(Object body) throws Exception {
        mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(body)))
                .andExpect(status().isBadRequest());
        assertThat(publisher.events).isEmpty();
    }

    static Stream<Arguments> invalidBatchBodies() {
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(Collections.singletonList(null)),
                Arguments.of(List.of(Map.of())),
                Arguments.of(Map.of()),
                Arguments.of((Object) null)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"[", ""})
    void rejectsMalformedJsonWithoutPublishing(String body) throws Exception {
        mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        assertThat(publisher.events).isEmpty();
    }

    @Test
    void normalizesTimestampWithOffset() throws Exception {
        publisher.confirmation.complete(null);
        var event = eventWith("timestamp", "2026-11-09T17:00:00-03:00");
        var result = mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(List.of(event))))
                .andExpect(request().asyncStarted()).andReturn();

        mvc.perform(asyncDispatch(result)).andExpect(status().isAccepted());
        assertThat(publisher.events.getFirst().timestamp()).isEqualTo(EVENT.timestamp());
    }

    @Test
    void validatesTheWholeBatchBeforePublishing() throws Exception {
        mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(List.of(EVENT, eventWith("level", "UNKNOWN")))))
                .andExpect(status().isBadRequest());
        assertThat(publisher.events).isEmpty();
    }

    @Test
    void rejectsInvalidFieldsBeforePublishing() throws Exception {
        ObjectNode unknownField = mapper.valueToTree(EVENT);
        unknownField.remove("logId");
        unknownField.put("unknownField", EVENT.logId());
        for (var invalid : List.of(eventWith("logId", " "),
                eventWith("serviceName", " "), eventWith("host", " "),
                eventWith("message", " "), eventWith("timestamp", "invalid"),
                eventWith("timestamp", "2026-11-09T17:00:00"), unknownField,
                eventWith("logId", "x".repeat(129)))) {
            mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                            .content(mapper.writeValueAsBytes(List.of(invalid))))
                    .andExpect(status().isBadRequest());
        }
        assertThat(publisher.events).isEmpty();
    }

    @Test
    void rejectsTheEntireBatchWhenALaterEventFailsBeanValidation() throws Exception {
        mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(List.of(EVENT, eventWith("serviceName", " ")))))
                .andExpect(status().isBadRequest());
        assertThat(publisher.events).isEmpty();
    }

    @Test
    void rejectsMoreThanOneThousandEvents() throws Exception {
        var body = Collections.nCopies(1001, EVENT);
        mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(body)))
                .andExpect(status().isBadRequest());
        assertThat(publisher.events).isEmpty();
    }

    @Test
    void rejectsOversizedRequestBeforeParsing() throws Exception {
        mvc.perform(post("/logs").servletPath("/logs").contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsBytes(List.of(eventWith("message", "x".repeat(1048576))))))
                .andExpect(status().isContentTooLarge())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.status").value(413))
                .andExpect(jsonPath("$.detail").value("The log batch exceeds the request size limit."));
        assertThat(publisher.events).isEmpty();
    }

    private ObjectNode eventWith(String field, String value) {
        ObjectNode event = mapper.valueToTree(EVENT);
        return event.put(field, value);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Configuration {
        @Bean
        TestPublisher publisher() {
            return new TestPublisher();
        }

        @Bean
        MeterRegistry registry() {
            return new SimpleMeterRegistry();
        }
    }

    static class TestPublisher implements LogPublisher {
        final List<LogEvent> events = new ArrayList<>();
        CompletableFuture<Void> confirmation;

        @Override
        public CompletableFuture<Void> publish(LogEvent event) {
            events.add(event);
            return confirmation;
        }
    }
}
