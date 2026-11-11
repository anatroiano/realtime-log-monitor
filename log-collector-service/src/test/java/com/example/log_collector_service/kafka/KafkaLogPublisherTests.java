package com.example.log_collector_service.kafka;

import com.example.log_collector_service.domain.LogEvent;
import com.example.log_collector_service.domain.LogLevel;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class KafkaLogPublisherTests {
    @Autowired
    ProducerFactory<String, LogEvent> factory;

    @Test
    void preservesTheWireContractAndWaitsForTheProducerCallback() {
        var properties = factory.getConfigurationProperties();
        assertThat(properties.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG).toString()).isEqualTo("true");
        assertThat(properties.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG).toString()).isEqualTo("30000");
        assertThat(properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(JacksonJsonSerializer.class);
        var serializer = new JacksonJsonSerializer<LogEvent>();
        serializer.configure(properties, false);
        var producer = new MockProducer<String, LogEvent>(false, null, new StringSerializer(), serializer);
        var template = new KafkaTemplate<>(new MockProducerFactory<>(() -> producer));
        var publisher = new KafkaLogPublisher(template, "logs.raw");
        var event = new LogEvent("stable-id", Instant.parse("2026-11-09T20:00:00Z"),
                "checkout", "host-1", LogLevel.ERROR, "Payment failed");

        var confirmation = publisher.publish(event);
        assertThat(confirmation).isNotDone();
        var record = producer.history().getFirst();
        assertThat(record.topic()).isEqualTo("logs.raw");
        assertThat(record.key()).isEqualTo("checkout");
        assertThat(record.timestamp()).isEqualTo(event.timestamp().toEpochMilli());
        var headers = new RecordHeaders();
        var json = JsonMapper.builder().build().readTree(serializer.serialize("logs.raw", headers, record.value()));
        assertThat(json.size()).isEqualTo(6);
        assertThat(json.get("logId").asString()).isEqualTo("stable-id");
        assertThat(json.get("timestamp").asString()).isEqualTo("2026-11-09T20:00:00Z");
        assertThat(json.get("serviceName").asString()).isEqualTo("checkout");
        assertThat(json.get("host").asString()).isEqualTo("host-1");
        assertThat(json.get("level").asString()).isEqualTo("ERROR");
        assertThat(json.get("message").asString()).isEqualTo("Payment failed");
        assertThat(headers.lastHeader("__TypeId__")).isNull();
        producer.completeNext();
        assertThat(confirmation).isCompleted();
        template.destroy();
        serializer.close();
    }
}
