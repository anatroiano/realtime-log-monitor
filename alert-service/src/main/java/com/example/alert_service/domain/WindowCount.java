package com.example.alert_service.domain;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "window_counts")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode(of = "id")
public class WindowCount {

    @EmbeddedId
    private WindowCountId id;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "total_logs", nullable = false)
    private long totalLogs;

    @Column(name = "error_count", nullable = false)
    private long errorCount;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
