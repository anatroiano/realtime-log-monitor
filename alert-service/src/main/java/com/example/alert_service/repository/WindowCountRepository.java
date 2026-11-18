package com.example.alert_service.repository;

import com.example.alert_service.domain.WindowCount;
import com.example.alert_service.domain.WindowCountId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface WindowCountRepository extends JpaRepository<WindowCount, WindowCountId> {

    @Modifying
    @Query(value = """
            INSERT INTO window_counts (service_name, window_start, window_end, total_logs, error_count, updated_at)
            VALUES (:serviceName, :windowStart, :windowEnd, :totalLogs, :errorCount, :updatedAt)
            ON CONFLICT (service_name, window_start) DO UPDATE
            SET window_end = EXCLUDED.window_end,
                total_logs = EXCLUDED.total_logs,
                error_count = EXCLUDED.error_count,
                updated_at = EXCLUDED.updated_at
            WHERE EXCLUDED.total_logs > window_counts.total_logs
            """, nativeQuery = true)
    int upsertIfNewer(@Param("serviceName") String serviceName,
                      @Param("windowStart") Instant windowStart,
                      @Param("windowEnd") Instant windowEnd,
                      @Param("totalLogs") long totalLogs,
                      @Param("errorCount") long errorCount,
                      @Param("updatedAt") Instant updatedAt);
}
