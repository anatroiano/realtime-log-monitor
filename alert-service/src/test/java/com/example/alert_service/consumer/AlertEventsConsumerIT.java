package com.example.alert_service.consumer;

import com.example.alert_service.TestcontainersConfiguration;
import com.example.alert_service.domain.WindowCountId;
import com.example.alert_service.repository.AlertRepository;
import com.example.alert_service.repository.WindowCountRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"logs.metrics", "alerts.triggered"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = {
                "auto.create.topics.enable=false",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        })
@Import(TestcontainersConfiguration.class)
@DirtiesContext
class AlertEventsConsumerIT {
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    EmbeddedKafkaBroker broker;
    @Autowired
    WindowCountRepository windowCountRepository;
    @Autowired
    AlertRepository alertRepository;

    private DefaultKafkaProducerFactory<String, String> producerFactory;
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        producerFactory = new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                new StringSerializer(), new StringSerializer());
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @AfterEach
    void tearDown() {
        kafkaTemplate.destroy();
        producerFactory.destroy();
    }

    @Test
    void keepsLatestWindowTotalsWhenAnOlderSnapshotIsRedelivered() throws Exception {
        send("logs.metrics", "inventory", metrics("inventory", 1, 1));
        send("logs.metrics", "inventory", metrics("inventory", 52, 51));
        send("logs.metrics", "inventory", metrics("inventory", 1, 1));
        send("logs.metrics", "inventory", metrics("inventory-marker", 1, 0));

        await().atMost(TIMEOUT).until(() ->
                windowCountRepository.existsById(new WindowCountId("inventory-marker", START)));
        var stored = windowCountRepository.findById(new WindowCountId("inventory", START)).orElseThrow();
        assertThat(stored.getTotalLogs()).isEqualTo(52);
        assertThat(stored.getErrorCount()).isEqualTo(51);
    }

    @Test
    void registersARedeliveredAlertOnce() throws Exception {
        var alertId = UUID.randomUUID();
        var markerId = UUID.randomUUID();

        send("alerts.triggered", alertId.toString(), alert(alertId, "checkout"));
        send("alerts.triggered", alertId.toString(), alert(alertId, "checkout"));
        send("alerts.triggered", alertId.toString(), alert(markerId, "checkout"));

        await().atMost(TIMEOUT).until(() -> alertRepository.existsById(markerId));
        assertThat(alertRepository.findAll()).filteredOn(alert -> alert.getAlertId().equals(alertId)).hasSize(1);
    }

    @Test
    void routesInvalidRecordsToTheDeadLetterTopicAndKeepsConsuming() throws Exception {
        var consumerProperties = KafkaTestUtils.consumerProps(broker, "dead-letter-reader", false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("logs.metrics.dlt"));

            send("logs.metrics", "billing", "not-json");
            send("logs.metrics", "billing", metrics("billing", 1, 2));
            send("logs.metrics", "billing", metrics("billing", 3, 1));

            var deadLetters = KafkaTestUtils.getRecords(consumer, TIMEOUT, 2);
            assertThat(deadLetters.count()).isEqualTo(2);
            for (var record : deadLetters) {
                assertThat(record.key()).isEqualTo("billing");
                assertThat(new String(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(),
                        StandardCharsets.UTF_8)).isEqualTo("logs.metrics");
                assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)).isNotNull();
                assertThat(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
                assertThat(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
            }
            assertThat(deadLetters).extracting(record -> record.value())
                    .containsExactlyInAnyOrder("not-json", metrics("billing", 1, 2));
        }
        await().atMost(TIMEOUT).until(() -> windowCountRepository.existsById(new WindowCountId("billing", START)));
        assertThat(windowCountRepository.findById(new WindowCountId("billing", START)).orElseThrow().getTotalLogs())
                .isEqualTo(3);
    }

    private void send(String topic, String key, String value) throws Exception {
        kafkaTemplate.send(topic, key, value).get(10, TimeUnit.SECONDS);
    }

    private static String metrics(String serviceName, long totalLogs, long errorCount) {
        return """
                {"serviceName":"%s","windowStart":"%s","windowEnd":"%s","totalLogs":%d,"errorCount":%d}"""
                .formatted(serviceName, START, START.plusSeconds(60), totalLogs, errorCount);
    }

    private static String alert(UUID alertId, String serviceName) {
        return """
                {"alertId":"%s","serviceName":"%s","ruleId":"errors-per-minute-over-50",\
                "windowStart":"%s","windowEnd":"%s","errorCount":51}"""
                .formatted(alertId, serviceName, START, START.plusSeconds(60));
    }
}
