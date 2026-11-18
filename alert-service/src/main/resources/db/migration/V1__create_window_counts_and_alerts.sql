CREATE TABLE window_counts
(
    service_name VARCHAR(128) NOT NULL,
    window_start TIMESTAMPTZ  NOT NULL,
    window_end   TIMESTAMPTZ  NOT NULL,
    total_logs   BIGINT       NOT NULL,
    error_count  BIGINT       NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_window_counts PRIMARY KEY (service_name, window_start),
    CONSTRAINT ck_window_counts_window CHECK (window_end > window_start),
    CONSTRAINT ck_window_counts_totals CHECK (error_count >= 0 AND error_count <= total_logs)
);

CREATE INDEX ix_window_counts_window_start ON window_counts (window_start);

CREATE TABLE alerts
(
    alert_id      UUID         NOT NULL,
    service_name  VARCHAR(128) NOT NULL,
    rule_id       VARCHAR(64)  NOT NULL,
    window_start  TIMESTAMPTZ  NOT NULL,
    window_end    TIMESTAMPTZ  NOT NULL,
    error_count   BIGINT       NOT NULL,
    registered_at TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_alerts PRIMARY KEY (alert_id),
    CONSTRAINT ck_alerts_window CHECK (window_end > window_start),
    CONSTRAINT ck_alerts_error_count CHECK (error_count > 0)
);

CREATE INDEX ix_alerts_window_start ON alerts (window_start DESC);
CREATE INDEX ix_alerts_service_window_start ON alerts (service_name, window_start DESC);
