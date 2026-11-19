package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LogTopologyTests {
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final ProcessorProperties settings = new ProcessorProperties("logs.raw", "logs.metrics",
            "alerts.triggered", "logs.raw.dlt", Duration.ofMinutes(5), Duration.ofHours(24),
            Duration.ofDays(7), Duration.ofSeconds(1), 3, (short) 1);

    @TempDir
    Path stateDirectory;
    private ValidatorFactory validators;
    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> input;
    private TestOutputTopic<String, WindowMetrics> metrics;
    private TestOutputTopic<String, AlertTriggered> alerts;
    private TestOutputTopic<String, RejectedLog> rejected;

    @BeforeEach
    void setUp() {
        validators = Validation.buildDefaultValidatorFactory();
        var builder = new StreamsBuilder();
        new LogTopologyConfiguration().logStream(builder, settings, mapper, validators.getValidator(), registry);
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        driver = new TopologyTestDriver(builder.build(), config);
        input = driver.createInputTopic(settings.rawTopic(), new StringSerializer(), new ByteArraySerializer());
        metrics = output(settings.metricsTopic(), WindowMetrics.class);
        alerts = output(settings.alertsTopic(), AlertTriggered.class);
        rejected = output(settings.deadLetterTopic(), RejectedLog.class);
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
        validators.close();
        registry.close();
    }

    @Test
    void countsEveryLevelAndUsesTheOriginalEventTimestamp() {
        for (var level : LogLevel.values()) {
            send(event(level.name(), "checkout", level, START.plusSeconds(30)));
        }
        assertThat(flushedMetrics()).containsExactly(new WindowMetrics(
                "checkout", START, START.plusSeconds(60), 6, 2));
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void deduplicatesByServiceAndLogId() {
        var log = event("same-id", "checkout", LogLevel.ERROR, START);
        send(log);
        send(log);
        send(event("same-id", "catalog", LogLevel.ERROR, START));

        assertThat(flushedMetrics()).hasSize(2).allSatisfy(metric -> {
            assertThat(metric.totalLogs()).isEqualTo(1);
            assertThat(metric.errorCount()).isEqualTo(1);
        });
        assertThat(counter("duplicate")).isEqualTo(1);
    }

    @Test
    void separatesAdjacentMinuteWindows() {
        send(event("first", "checkout", LogLevel.ERROR, START.plusMillis(59999)));
        send(event("second", "checkout", LogLevel.INFO, START.plusSeconds(60)));

        assertThat(flushedMetrics()).containsExactlyInAnyOrder(
                new WindowMetrics("checkout", START, START.plusSeconds(60), 1, 1),
                new WindowMetrics("checkout", START.plusSeconds(60), START.plusSeconds(120), 1, 0));
    }

    @Test
    void alertsOnTheFiftyFirstErrorAndOnlyOncePerWindow() {
        for (int i = 0; i < 50; i++) {
            send(event("error-" + i, "checkout", LogLevel.ERROR, START));
        }
        assertThat(alerts.isEmpty()).isTrue();
        send(event("fatal", "checkout", LogLevel.FATAL, START));

        var alert = alerts.readKeyValue();
        assertThat(alert.key).isEqualTo(alert.value.alertId());
        assertThat(alert.value.serviceName()).isEqualTo("checkout");
        assertThat(alert.value.ruleId()).isEqualTo("errors-per-minute-over-50");
        assertThat(alert.value.windowStart()).isEqualTo(START);
        assertThat(alert.value.windowEnd()).isEqualTo(START.plusSeconds(60));
        assertThat(alert.value.errorCount()).isEqualTo(51);
        assertThat(alert.value.alertId()).isEqualTo("dbf74ea9-cea9-3f5c-a7fe-6d22a9bfeba1");
        assertThat(AlertTriggered.from(new WindowMetrics("checkout", START, START.plusSeconds(60), 53, 52))
                .alertId()).isEqualTo(alert.value.alertId());

        send(event("info", "checkout", LogLevel.INFO, START));
        send(event("error-52", "checkout", LogLevel.ERROR, START));
        send(event("fatal", "checkout", LogLevel.FATAL, START));
        assertThat(alerts.isEmpty()).isTrue();
        assertThat(flushedMetrics()).containsExactly(
                new WindowMetrics("checkout", START, START.plusSeconds(60), 53, 52));
    }

    @Test
    void emitsDistinctAlertsInConsecutiveWindows() {
        for (int minute = 0; minute < 2; minute++) {
            for (int i = 0; i < 51; i++) {
                send(event(minute + "-" + i, "checkout", LogLevel.ERROR, START.plusSeconds(minute * 60L)));
            }
        }
        var incidents = alerts.readValuesToList();
        assertThat(incidents).hasSize(2);
        assertThat(incidents.getFirst().alertId()).isNotEqualTo(incidents.getLast().alertId());
    }

    @Test
    void updatesAnOlderWindowWithinTheGracePeriod() {
        send(event("first", "checkout", LogLevel.INFO, START));
        send(event("advance", "catalog", LogLevel.INFO, START.plusSeconds(359)));
        send(event("late", "checkout", LogLevel.ERROR, START.plusSeconds(30)));

        assertThat(flushedMetrics()).contains(
                new WindowMetrics("checkout", START, START.plusSeconds(60), 2, 1));
        assertThat(counter("late")).isZero();
    }

    @Test
    void dropsAnEventAtTheGraceDeadline() {
        send(event("advance", "catalog", LogLevel.INFO, START.plusSeconds(360)));
        flushedMetrics();
        send(event("too-late", "checkout", LogLevel.ERROR, START.plusMillis(59999)));

        assertThat(flushedMetrics()).isEmpty();
        assertThat(counter("late")).isEqualTo(1);
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void routesInvalidJsonToDltAndContinuesWithTheNextEvent() {
        byte[] invalid = "[".getBytes(StandardCharsets.UTF_8);
        input.pipeInput("checkout", invalid, START);
        send(event("valid", "checkout", LogLevel.INFO, START));

        var failure = rejected.readValue();
        assertThat(failure.topic()).isEqualTo("logs.raw");
        assertThat(failure.partition()).isZero();
        assertThat(failure.offset()).isZero();
        assertThat(failure.key()).isEqualTo("checkout");
        assertThat(failure.value()).isEqualTo(invalid);
        assertThat(failure.reason()).isEqualTo("INVALID_JSON");
        assertThat(flushedMetrics()).singleElement().extracting(WindowMetrics::totalLogs).isEqualTo(1L);
        assertThat(counter("invalid")).isEqualTo(1);
    }

    @Test
    void rejectsInvalidFieldsWithoutAdvancingStreamTime() {
        send(event("first", "checkout", LogLevel.INFO, START));
        ObjectNode invalid = mapper.valueToTree(event("invalid", "checkout", LogLevel.INFO,
                START.plus(Duration.ofDays(1))));
        invalid.put("message", " ");
        input.pipeInput("checkout", mapper.writeValueAsBytes(invalid), START.plus(Duration.ofDays(1)));
        send(event("second", "checkout", LogLevel.ERROR, START.plusSeconds(10)));

        assertThat(rejected.readValue().reason()).isEqualTo("INVALID_FIELDS");
        assertThat(flushedMetrics()).singleElement().extracting(WindowMetrics::totalLogs).isEqualTo(2L);
        assertThat(counter("late")).isZero();
    }

    @Test
    void rejectsMismatchedKeysWithoutAdvancingStreamTime() {
        var log = event("mismatch", "checkout", LogLevel.ERROR, START.plus(Duration.ofDays(1)));
        input.pipeInput("wrong-key", mapper.writeValueAsBytes(log), START.plus(Duration.ofDays(1)));
        send(event("valid", "checkout", LogLevel.INFO, START));

        assertThat(rejected.readValue().reason()).isEqualTo("KEY_MISMATCH");
        assertThat(flushedMetrics()).singleElement().extracting(WindowMetrics::totalLogs).isEqualTo(1L);
        assertThat(counter("late")).isZero();
    }

    @Test
    void routesTombstonesToDlt() {
        input.pipeInput("checkout", null, START);

        var failure = rejected.readValue();
        assertThat(failure.reason()).isEqualTo("MISSING_VALUE");
        assertThat(failure.value()).isNull();
        assertThat(flushedMetrics()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("unsupportedTimestamps")
    void rejectsTimestampsOutsideTheKafkaRange(Instant timestamp) {
        send(event("invalid-timestamp", "checkout", LogLevel.ERROR, timestamp));

        assertThat(rejected.readValue().reason()).isEqualTo("INVALID_TIMESTAMP");
        assertThat(flushedMetrics()).isEmpty();
    }

    static Stream<Instant> unsupportedTimestamps() {
        return Stream.of(Instant.EPOCH.minusMillis(1), Instant.MAX);
    }

    @Test
    void usesRestoredDeduplicationAndAggregationState() {
        WindowStore<String, Long> seen = driver.getWindowStore(LogRecordProcessor.SEEN_LOGS_STORE);
        WindowStore<String, ValueAndTimestamp<WindowCounts>> counts = driver.getTimestampedWindowStore("log-counts");
        seen.put(EventKeys.log("checkout", "restored"), START.toEpochMilli(), START.toEpochMilli());
        counts.put("checkout", ValueAndTimestamp.make(new WindowCounts(51, 51, true), START.toEpochMilli()),
                START.toEpochMilli());

        send(event("restored", "checkout", LogLevel.ERROR, START));
        assertThat(flushedMetrics()).isEmpty();
        send(event("new", "checkout", LogLevel.FATAL, START));

        assertThat(flushedMetrics()).containsExactly(
                new WindowMetrics("checkout", START, START.plusSeconds(60), 52, 52));
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void distinguishesCompositeKeysContainingSeparators() {
        send(event("c", "a:b", LogLevel.INFO, START));
        send(event("b:c", "a", LogLevel.INFO, START));

        driver.advanceWallClockTime(settings.metricsFlushInterval());
        assertThat(metrics.readKeyValuesToMap()).hasSize(2);
        assertThat(counter("accepted")).isEqualTo(2);
        assertThat(counter("duplicate")).isZero();
    }

    @Test
    void expiresDeduplicationStateAfterTheConfiguredRetention() {
        send(event("reused-id", "checkout", LogLevel.INFO, START));
        Instant nextDay = START.plus(Duration.ofHours(24)).plusMillis(1);
        send(event("advance", "catalog", LogLevel.INFO, nextDay));
        send(event("reused-id", "checkout", LogLevel.ERROR, nextDay));

        assertThat(counter("accepted")).isEqualTo(3);
        assertThat(counter("duplicate")).isZero();
    }

    @Test
    void serializesMetricKeysAndValuesWithoutJavaTypeHeaders() {
        var wire = driver.createOutputTopic(settings.metricsTopic(),
                new StringDeserializer(), new ByteArrayDeserializer());
        send(event("wire-contract", "checkout", LogLevel.ERROR, START));
        driver.advanceWallClockTime(settings.metricsFlushInterval());

        var record = wire.readRecord();
        assertThat(record.key()).isEqualTo("Y2hlY2tvdXQ:" + START.toEpochMilli());
        var json = mapper.readTree(record.value());
        assertThat(json.size()).isEqualTo(5);
        assertThat(json.get("windowStart").asString()).isEqualTo(START.toString());
        assertThat(json.get("windowEnd").asString()).isEqualTo(START.plusSeconds(60).toString());
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
    }

    @Test
    void publishesOnlyTheLatestSnapshotPerWindowAtEachFlush() {
        send(event("first", "checkout", LogLevel.INFO, START));
        send(event("second", "checkout", LogLevel.ERROR, START));
        send(event("third", "checkout", LogLevel.ERROR, START));

        assertThat(metrics.isEmpty()).isTrue();
        assertThat(flushedMetrics()).containsExactly(
                new WindowMetrics("checkout", START, START.plusSeconds(60), 3, 2));
        assertThat(flushedMetrics()).isEmpty();
    }

    @Test
    void publishesAlertsBeforeTheMetricsFlush() {
        for (int i = 0; i < 51; i++) {
            send(event("error-" + i, "checkout", LogLevel.ERROR, START));
        }

        assertThat(alerts.readValue().errorCount()).isEqualTo(51);
        assertThat(metrics.isEmpty()).isTrue();
    }

    @Test
    void keepsPendingSnapshotsInTheStateStoreUntilTheFlush() {
        send(event("pending", "checkout", LogLevel.ERROR, START));
        KeyValueStore<String, WindowMetrics> pending =
                driver.getKeyValueStore(MetricsSnapshotBuffer.PENDING_METRICS_STORE);

        assertThat(pending.get(EventKeys.window("checkout", START))).isEqualTo(
                new WindowMetrics("checkout", START, START.plusSeconds(60), 1, 1));
        flushedMetrics();
        try (var remaining = pending.all()) {
            assertThat(remaining.hasNext()).isFalse();
        }
    }

    private List<WindowMetrics> flushedMetrics() {
        driver.advanceWallClockTime(settings.metricsFlushInterval());
        return metrics.readValuesToList();
    }

    private LogEvent event(String id, String service, LogLevel level, Instant timestamp) {
        return new LogEvent(id, timestamp, service, "host-1", level, "Application event");
    }

    private void send(LogEvent event) {
        input.pipeInput(event.serviceName(), mapper.writeValueAsBytes(event), START.plus(Duration.ofDays(7)));
    }

    private double counter(String result) {
        return registry.get("processor.logs").tag("result", result).counter().count();
    }

    private <T> TestOutputTopic<String, T> output(String topic, Class<T> type) {
        var serde = new JacksonJsonSerde<T>(type, mapper).ignoreTypeHeaders();
        return driver.createOutputTopic(topic, new StringDeserializer(), serde.deserializer());
    }
}
