package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.kafka.streams.application-id=processor-integration-test")
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers", brokerProperties = {
        "auto.create.topics.enable=false",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "offsets.topic.replication.factor=1"
})
@DirtiesContext
class LogProcessingIT {
    @TempDir
    static Path stateDirectory;
    @Autowired
    EmbeddedKafkaBroker broker;
    @Autowired
    JsonMapper mapper;
    @Autowired
    ProcessorProperties properties;

    @DynamicPropertySource
    static void streamsStateDirectory(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.streams.state-dir", () -> stateDirectory.toString());
    }

    @Test
    void commitsDeduplicatedMetricsAndOneAlertToKafka() throws Exception {
        Instant windowStart = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        List<LogEvent> logs = IntStream.range(0, 51)
                .mapToObj(id -> new LogEvent("error-" + id, windowStart.plusSeconds(10),
                        "checkout", "host-1", LogLevel.ERROR, "Payment failed"))
                .toList();
        var producerFactory = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                new StringSerializer(), new JacksonJsonSerializer<LogEvent>(mapper).noTypeInfo());
        var template = new KafkaTemplate<>(producerFactory);
        var consumerProperties = KafkaTestUtils.consumerProps(broker, "processor-integration-consumer", false);
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<>(consumerProperties,
                new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(properties.metricsTopic(), properties.alertsTopic()));
            var publications = new ArrayList<CompletableFuture<?>>();
            for (LogEvent event : logs) {
                publications.add(template.send(properties.rawTopic(), event.serviceName(), event));
            }
            var info = new LogEvent("info", windowStart.plusSeconds(20), "checkout", "host-1",
                    LogLevel.INFO, "Request completed");
            publications.add(template.send(properties.rawTopic(), info.serviceName(), info));
            publications.add(template.send(properties.rawTopic(), logs.getFirst().serviceName(), logs.getFirst()));
            CompletableFuture.allOf(publications.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);

            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(30), 53);
            var snapshots = new ArrayList<WindowMetrics>();
            var alerts = new ArrayList<AlertTriggered>();
            for (var record : records) {
                assertThat(record.headers().lastHeader("__TypeId__")).isNull();
                if (record.topic().equals(properties.metricsTopic())) {
                    snapshots.add(mapper.readValue(record.value(), WindowMetrics.class));
                    assertThat(record.key()).isEqualTo(EventKeys.window("checkout", windowStart));
                } else {
                    var alert = mapper.readValue(record.value(), AlertTriggered.class);
                    assertThat(record.key()).isEqualTo(alert.alertId());
                    alerts.add(alert);
                }
            }
            assertThat(snapshots).hasSize(52);
            assertThat(snapshots.getLast()).isEqualTo(
                    new WindowMetrics("checkout", windowStart, windowStart.plusSeconds(60), 52, 51));
            assertThat(alerts).hasSize(1);
            assertThat(alerts.getFirst().errorCount()).isEqualTo(51);
        } finally {
            template.destroy();
            producerFactory.destroy();
        }
    }
}
