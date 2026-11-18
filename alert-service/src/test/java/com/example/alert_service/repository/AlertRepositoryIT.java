package com.example.alert_service.repository;

import com.example.alert_service.TestcontainersConfiguration;
import com.example.alert_service.domain.Alert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration.class)
class AlertRepositoryIT {
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final Instant FIRST_DELIVERY = Instant.parse("2026-11-11T12:01:00Z");
    private static final Instant REDELIVERY = Instant.parse("2026-11-11T12:30:00Z");

    @Autowired
    AlertRepository alertRepository;
    @Autowired
    TestEntityManager entityManager;

    @Test
    void registersARedeliveredAlertOnlyOnce() {
        var alertId = UUID.randomUUID();

        assertThat(insert(alertId, "checkout", START, FIRST_DELIVERY)).isEqualTo(1);
        assertThat(insert(alertId, "checkout", START, REDELIVERY)).isZero();

        entityManager.flush();
        entityManager.clear();
        assertThat(alertRepository.count()).isEqualTo(1);
        assertThat(alertRepository.findById(alertId).orElseThrow().getRegisteredAt()).isEqualTo(FIRST_DELIVERY);
    }

    @Test
    void pagesHistoryOfOneServiceByMostRecentWindow() {
        insert(UUID.randomUUID(), "checkout", START, FIRST_DELIVERY);
        insert(UUID.randomUUID(), "checkout", START.plusSeconds(60), FIRST_DELIVERY);
        insert(UUID.randomUUID(), "payments", START.plusSeconds(120), FIRST_DELIVERY);
        entityManager.clear();

        var page = alertRepository.findByServiceName("checkout",
                PageRequest.of(0, 10, Sort.by(Sort.Direction.DESC, "windowStart")));

        assertThat(page.getTotalElements()).isEqualTo(2);
        assertThat(page.getContent()).extracting(Alert::getWindowStart)
                .containsExactly(START.plusSeconds(60), START);
    }

    private int insert(UUID alertId, String serviceName, Instant windowStart, Instant registeredAt) {
        return alertRepository.insertIfAbsent(alertId, serviceName, "errors-per-minute-over-50",
                windowStart, windowStart.plusSeconds(60), 51, registeredAt);
    }
}
