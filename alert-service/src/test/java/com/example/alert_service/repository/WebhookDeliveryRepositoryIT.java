package com.example.alert_service.repository;

import com.example.alert_service.TestcontainersConfiguration;
import com.example.alert_service.domain.WebhookDelivery;
import com.example.alert_service.domain.WebhookDeliveryStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration.class)
class WebhookDeliveryRepositoryIT {
    private static final Instant NOW = Instant.parse("2026-11-11T12:05:00Z");
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");

    @Autowired
    WebhookDeliveryRepository webhookDeliveryRepository;
    @Autowired
    AlertRepository alertRepository;
    @Autowired
    TestEntityManager entityManager;

    @Test
    void findsOnlyDuePendingDeliveriesOldestFirst() {
        var later = save(WebhookDeliveryStatus.PENDING, NOW.minusSeconds(10));
        var earlier = save(WebhookDeliveryStatus.PENDING, NOW.minusSeconds(60));
        save(WebhookDeliveryStatus.PENDING, NOW.plusSeconds(1));
        save(WebhookDeliveryStatus.SENT, NOW.minusSeconds(120));
        save(WebhookDeliveryStatus.FAILED, NOW.minusSeconds(120));
        entityManager.flush();
        entityManager.clear();

        var due = webhookDeliveryRepository.findDueForUpdate(NOW, PageRequest.of(0, 10));

        assertThat(due).extracting(WebhookDelivery::getAlertId).containsExactly(earlier, later);
    }

    @Test
    void limitsTheBatchSize() {
        save(WebhookDeliveryStatus.PENDING, NOW.minusSeconds(3));
        save(WebhookDeliveryStatus.PENDING, NOW.minusSeconds(2));
        save(WebhookDeliveryStatus.PENDING, NOW.minusSeconds(1));
        entityManager.flush();

        assertThat(webhookDeliveryRepository.findDueForUpdate(NOW, PageRequest.of(0, 2))).hasSize(2);
    }

    @Test
    void persistsStatusTransitionsThroughDirtyChecking() {
        var alertId = save(WebhookDeliveryStatus.PENDING, NOW);
        entityManager.flush();
        entityManager.clear();

        webhookDeliveryRepository.findById(alertId).orElseThrow().markSent(NOW.plusSeconds(1));
        entityManager.flush();
        entityManager.clear();

        var delivery = webhookDeliveryRepository.findById(alertId).orElseThrow();
        assertThat(delivery.getStatus()).isEqualTo(WebhookDeliveryStatus.SENT);
        assertThat(delivery.getSentAt()).isEqualTo(NOW.plusSeconds(1));
        assertThat(delivery.getAttempts()).isEqualTo(1);
    }

    @Test
    void requiresAnExistingAlert() {
        var delivery = WebhookDelivery.pending(UUID.randomUUID(), "{}", NOW);

        assertThatThrownBy(() -> webhookDeliveryRepository.saveAndFlush(delivery))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private UUID save(WebhookDeliveryStatus status, Instant nextAttemptAt) {
        var alertId = UUID.randomUUID();
        alertRepository.insertIfAbsent(alertId, "checkout", "errors-per-minute-over-50", START,
                START.plusSeconds(60), 51, START);
        var sentAt = status == WebhookDeliveryStatus.SENT ? nextAttemptAt : null;
        webhookDeliveryRepository.save(new WebhookDelivery(alertId, "{}", status, 0, nextAttemptAt, START,
                sentAt, null));
        return alertId;
    }
}
