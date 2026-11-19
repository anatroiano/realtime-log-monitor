package com.example.alert_service.repository;

import com.example.alert_service.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration.class)
class NotificationCooldownRepositoryIT {
    private static final Duration COOLDOWN = Duration.ofMinutes(5);
    private static final Instant FIRST = Instant.parse("2026-11-11T12:00:00Z");

    @Autowired
    NotificationCooldownRepository notificationCooldownRepository;
    @Autowired
    TestEntityManager entityManager;

    @Test
    void allowsOneNotificationPerServiceWithinTheCooldown() {
        assertThat(claim("checkout", FIRST)).isEqualTo(1);
        assertThat(claim("checkout", FIRST.plus(COOLDOWN).minusMillis(1))).isZero();
        assertThat(claim("payments", FIRST.plusSeconds(1))).isEqualTo(1);
        assertThat(claim("checkout", FIRST.plus(COOLDOWN))).isEqualTo(1);

        entityManager.flush();
        entityManager.clear();
        assertThat(notificationCooldownRepository.findById("checkout").orElseThrow().getLastNotifiedAt())
                .isEqualTo(FIRST.plus(COOLDOWN));
    }

    @Test
    void suppressedClaimsDoNotExtendTheCooldown() {
        claim("checkout", FIRST);
        claim("checkout", FIRST.plusSeconds(60));

        entityManager.clear();
        assertThat(notificationCooldownRepository.findById("checkout").orElseThrow().getLastNotifiedAt())
                .isEqualTo(FIRST);
    }

    private int claim(String serviceName, Instant notifiedAt) {
        return notificationCooldownRepository.claimNotification(serviceName, notifiedAt, notifiedAt.minus(COOLDOWN));
    }
}
