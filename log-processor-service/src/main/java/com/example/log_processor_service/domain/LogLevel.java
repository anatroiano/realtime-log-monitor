package com.example.log_processor_service.domain;

public enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL;

    public boolean isError() {
        return this == ERROR || this == FATAL;
    }
}
