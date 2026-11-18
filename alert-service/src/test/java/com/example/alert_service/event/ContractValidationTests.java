package com.example.alert_service.event;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ContractValidationTests {
    private static final Instant START = Instant.parse("2026-11-11T12:00:00Z");
    private static final Instant END = START.plusSeconds(60);

    private ValidatorFactory factory;
    private Validator validator;

    @BeforeEach
    void setUp() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterEach
    void tearDown() {
        factory.close();
    }

    @Test
    void acceptsCompleteWindowMetrics() {
        assertThat(validator.validate(new WindowMetrics("checkout", START, END, 52L, 51L))).isEmpty();
    }

    @Test
    void rejectsWindowMetricsWithMissingTotals() {
        assertThat(validator.validate(new WindowMetrics("checkout", START, END, null, 1L))).isNotEmpty();
        assertThat(validator.validate(new WindowMetrics("checkout", START, END, 1L, null))).isNotEmpty();
    }

    @Test
    void rejectsMoreErrorsThanLogs() {
        assertThat(validator.validate(new WindowMetrics("checkout", START, END, 50L, 51L)))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactly("errorCountWithinTotal");
    }

    @Test
    void rejectsWindowEndingBeforeItStarts() {
        assertThat(validator.validate(new WindowMetrics("checkout", END, START, 1L, 0L)))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactly("windowOrdered");
        assertThat(validator.validate(new AlertTriggered(UUID.randomUUID(), "checkout", "rule", END, START, 51L)))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactly("windowOrdered");
    }

    @Test
    void rejectsAlertWithoutIdentityOrErrors() {
        assertThat(validator.validate(new AlertTriggered(null, "checkout", "rule", START, END, 51L))).isNotEmpty();
        assertThat(validator.validate(new AlertTriggered(UUID.randomUUID(), "checkout", "rule", START, END, 0L)))
                .isNotEmpty();
        assertThat(validator.validate(new AlertTriggered(UUID.randomUUID(), " ", "rule", START, END, 51L)))
                .isNotEmpty();
    }
}
