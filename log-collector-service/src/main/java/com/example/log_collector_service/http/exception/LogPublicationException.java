package com.example.log_collector_service.http.exception;

public class LogPublicationException extends RuntimeException {
    public LogPublicationException(Throwable cause) {
        super("Batch publication could not be confirmed", cause);
    }
}
