package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.*;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
@EnableConfigurationProperties(ProcessorProperties.class)
public class LogTopologyConfiguration {
    @Bean
    public KStream<String, WindowMetrics> logStream(StreamsBuilder builder, ProcessorProperties properties,
                                                    JsonMapper mapper, Validator validator, MeterRegistry registry) {
        registerStateStores(builder, properties);
        var processed = readLogs(builder, properties, mapper, validator, registry);
        publishRejectedLogs(processed, properties, mapper);

        var counts = aggregateWindows(processed, properties, mapper);
        var metrics = publishMetrics(counts, properties, mapper);
        publishAlerts(counts, properties, mapper);
        return metrics;
    }

    private void registerStateStores(StreamsBuilder builder, ProcessorProperties properties) {
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(LogRecordProcessor.SEEN_LOGS_STORE,
                        properties.deduplicationRetention(), properties.deduplicationRetention(), false),
                Serdes.String(), Serdes.Long()));
    }

    private KStream<String, ProcessedLog> readLogs(StreamsBuilder builder, ProcessorProperties properties,
                                                   JsonMapper mapper, Validator validator, MeterRegistry registry) {
        Serde<DecodedLog> inputSerde = Serdes.serdeFrom((topic, log) -> log.originalValue(),
                new LogRecordDeserializer(mapper, validator));
        return builder.stream(properties.rawTopic(),
                        Consumed.with(Serdes.String(), inputSerde).withTimestampExtractor(new LogTimestampExtractor()))
                .processValues(() -> new LogRecordProcessor(properties, registry),
                        Named.as("validate-and-deduplicate"), LogRecordProcessor.SEEN_LOGS_STORE);
    }

    private void publishRejectedLogs(KStream<String, ProcessedLog> processed, ProcessorProperties properties,
                                     JsonMapper mapper) {
        processed.filter((key, value) -> value.rejection() != null)
                .mapValues(ProcessedLog::rejection)
                .to(properties.deadLetterTopic(), Produced.with(Serdes.String(), jsonSerde(RejectedLog.class, mapper)));
    }

    private KStream<Windowed<String>, WindowCounts> aggregateWindows(KStream<String, ProcessedLog> processed,
                                                                     ProcessorProperties properties, JsonMapper mapper) {
        return processed.filter((key, value) -> value.event() != null)
                .mapValues(ProcessedLog::event)
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde(LogEvent.class, mapper)))
                .windowedBy(TimeWindows.ofSizeAndGrace(ProcessorProperties.WINDOW_SIZE, properties.gracePeriod()))
                .aggregate(WindowCounts::empty, (service, event, previous) -> previous.add(event),
                        Materialized.<String, WindowCounts, WindowStore<Bytes, byte[]>>as("log-counts")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde(WindowCounts.class, mapper))
                                .withRetention(properties.deduplicationRetention())
                                .withCachingDisabled())
                .toStream();
    }

    private KStream<String, WindowMetrics> publishMetrics(KStream<Windowed<String>, WindowCounts> counts,
                                                          ProcessorProperties properties, JsonMapper mapper) {
        var metrics = counts.map((window, value) -> {
            var metric = windowMetrics(window, value);
            return KeyValue.pair(EventKeys.window(metric.serviceName(), metric.windowStart()), metric);
        });
        metrics.to(properties.metricsTopic(), Produced.with(Serdes.String(), jsonSerde(WindowMetrics.class, mapper)));
        return metrics;
    }

    private void publishAlerts(KStream<Windowed<String>, WindowCounts> counts, ProcessorProperties properties,
                               JsonMapper mapper) {
        counts.filter((window, value) -> value.thresholdCrossed())
                .map((window, value) -> {
                    var alert = AlertTriggered.from(windowMetrics(window, value));
                    return KeyValue.pair(alert.alertId(), alert);
                })
                .to(properties.alertsTopic(), Produced.with(Serdes.String(), jsonSerde(AlertTriggered.class, mapper)));
    }

    private <T> JacksonJsonSerde<T> jsonSerde(Class<T> type, JsonMapper mapper) {
        var serde = new JacksonJsonSerde<T>(type, mapper);
        serde.noTypeInfo();
        serde.ignoreTypeHeaders();
        return serde;
    }

    private WindowMetrics windowMetrics(Windowed<String> window, WindowCounts counts) {
        return new WindowMetrics(window.key(), Instant.ofEpochMilli(window.window().start()),
                Instant.ofEpochMilli(window.window().end()), counts.totalLogs(), counts.errorCount());
    }
}
