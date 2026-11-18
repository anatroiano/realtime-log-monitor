package com.example.alert_service.repository;

import com.example.alert_service.TestcontainersConfiguration;
import com.example.alert_service.domain.WindowCount;
import com.example.alert_service.domain.WindowCountId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@ImportAutoConfiguration(FlywayAutoConfiguration.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration.class)
class WindowCountRepositoryIT {
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final Instant END = START.plusSeconds(60);
    private static final Instant FIRST_UPDATE = Instant.parse("2026-11-11T12:00:10Z");
    private static final Instant SECOND_UPDATE = Instant.parse("2026-11-11T12:00:20Z");

    @Autowired
    WindowCountRepository windowCountRepository;
    @Autowired
    TestEntityManager entityManager;

    @Test
    void insertsTheFirstSnapshotOfAWindow() {
        assertThat(windowCountRepository.upsertIfNewer("checkout", START, END, 1, 1, FIRST_UPDATE)).isEqualTo(1);

        var stored = reload("checkout");
        assertThat(stored.getWindowEnd()).isEqualTo(END);
        assertThat(stored.getTotalLogs()).isEqualTo(1);
        assertThat(stored.getErrorCount()).isEqualTo(1);
        assertThat(stored.getUpdatedAt()).isEqualTo(FIRST_UPDATE);
    }

    @Test
    void replacesTotalsWithALargerSnapshotWithoutSummingThem() {
        windowCountRepository.upsertIfNewer("checkout", START, END, 10, 3, FIRST_UPDATE);

        assertThat(windowCountRepository.upsertIfNewer("checkout", START, END, 52, 51, SECOND_UPDATE)).isEqualTo(1);

        var stored = reload("checkout");
        assertThat(stored.getTotalLogs()).isEqualTo(52);
        assertThat(stored.getErrorCount()).isEqualTo(51);
        assertThat(stored.getUpdatedAt()).isEqualTo(SECOND_UPDATE);
    }

    @Test
    void keepsTheLatestTotalsWhenAnOlderOrRepeatedSnapshotIsRedelivered() {
        windowCountRepository.upsertIfNewer("checkout", START, END, 52, 51, FIRST_UPDATE);

        assertThat(windowCountRepository.upsertIfNewer("checkout", START, END, 10, 3, SECOND_UPDATE)).isZero();
        assertThat(windowCountRepository.upsertIfNewer("checkout", START, END, 52, 51, SECOND_UPDATE)).isZero();

        var stored = reload("checkout");
        assertThat(stored.getTotalLogs()).isEqualTo(52);
        assertThat(stored.getErrorCount()).isEqualTo(51);
        assertThat(stored.getUpdatedAt()).isEqualTo(FIRST_UPDATE);
    }

    @Test
    void keepsWindowsOfDifferentServicesAndMinutesApart() {
        windowCountRepository.upsertIfNewer("checkout", START, END, 5, 1, FIRST_UPDATE);
        windowCountRepository.upsertIfNewer("payments", START, END, 7, 2, FIRST_UPDATE);
        windowCountRepository.upsertIfNewer("checkout", END, END.plusSeconds(60), 3, 0, FIRST_UPDATE);

        assertThat(windowCountRepository.count()).isEqualTo(3);
        assertThat(reload("payments").getTotalLogs()).isEqualTo(7);
    }

    @Test
    void rejectsMoreErrorsThanLogs() {
        assertThatThrownBy(() -> windowCountRepository.upsertIfNewer("checkout", START, END, 1, 2, FIRST_UPDATE))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private WindowCount reload(String serviceName) {
        entityManager.flush();
        entityManager.clear();
        return windowCountRepository.findById(new WindowCountId(serviceName, START)).orElseThrow();
    }
}
