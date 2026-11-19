CREATE TABLE webhook_deliveries
(
    alert_id        UUID         NOT NULL,
    payload         TEXT         NOT NULL,
    status          VARCHAR(16)  NOT NULL,
    attempts        INTEGER      NOT NULL,
    next_attempt_at TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    sent_at         TIMESTAMPTZ,
    last_error      VARCHAR(255),
    CONSTRAINT pk_webhook_deliveries PRIMARY KEY (alert_id),
    CONSTRAINT fk_webhook_deliveries_alert FOREIGN KEY (alert_id) REFERENCES alerts (alert_id),
    CONSTRAINT ck_webhook_deliveries_status CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    CONSTRAINT ck_webhook_deliveries_attempts CHECK (attempts >= 0),
    CONSTRAINT ck_webhook_deliveries_sent_at CHECK ((status = 'SENT') = (sent_at IS NOT NULL))
);

CREATE INDEX ix_webhook_deliveries_pending ON webhook_deliveries (next_attempt_at) WHERE status = 'PENDING';

CREATE TABLE notification_cooldowns
(
    service_name     VARCHAR(128) NOT NULL,
    last_notified_at TIMESTAMPTZ  NOT NULL,
    CONSTRAINT pk_notification_cooldowns PRIMARY KEY (service_name)
);
