package com.example.log_collector_service.kafka;

import com.example.log_collector_service.domain.LogEvent;
import com.example.log_collector_service.domain.LogLevel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = "auto.create.topics.enable=false")
@DirtiesContext
class KafkaPublicationIT {
    @Autowired
    MockMvc mvc;
    @Autowired
    EmbeddedKafkaBroker broker;
    @Autowired
    JsonMapper mapper;

    @Test
    void publishesJsonWithServiceKeyOriginalTimestampAndUnchangedIdsOnRedelivery() throws Exception {
        var properties = KafkaTestUtils.consumerProps(broker, "collector-contract-test", false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("logs.raw"));
            var logEvent = new LogEvent(
                    "stable-id",
                    Instant.parse("2026-11-09T20:00:00Z"),
                    "checkout",
                    "host-1",
                    LogLevel.ERROR,
                    "Payment failed"
            );
            byte[] batch = mapper.writeValueAsBytes(List.of(logEvent));
            for (int i = 0; i < 2; i++) {
                var result = mvc.perform(post("/logs").contentType(MediaType.APPLICATION_JSON).content(batch))
                        .andExpect(request().asyncStarted()).andReturn();
                mvc.perform(asyncDispatch(result)).andExpect(status().isAccepted());
            }

            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), 2);
            assertThat(records.count()).isEqualTo(2);
            for (var record : records) {
                assertThat(record.key()).isEqualTo("checkout");
                var event = mapper.readTree(record.value());
                assertThat(event.size()).isEqualTo(6);
                assertThat(event.get("logId").asString()).isEqualTo("stable-id");
                assertThat(event.get("timestamp").asString()).isEqualTo("2026-11-09T20:00:00Z");
                assertThat(event.get("level").asString()).isEqualTo("ERROR");
                assertThat(record.timestamp()).isEqualTo(logEvent.timestamp().toEpochMilli());
                assertThat(record.headers().lastHeader("__TypeId__")).isNull();
            }
        }
    }
}
