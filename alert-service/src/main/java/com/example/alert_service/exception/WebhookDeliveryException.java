package com.example.alert_service.exception;

import lombok.Getter;

@Getter
public class WebhookDeliveryException extends RuntimeException {
    private final boolean retryable;

    public WebhookDeliveryException(String reason, boolean retryable, Throwable cause) {
        super(reason, cause);
        this.retryable = retryable;
    }
}
