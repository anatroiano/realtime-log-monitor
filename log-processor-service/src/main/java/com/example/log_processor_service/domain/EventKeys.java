package com.example.log_processor_service.domain;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public final class EventKeys {
    private EventKeys() {
    }

    public static String log(String serviceName, String logId) {
        return encode(serviceName) + ":" + encode(logId);
    }

    public static String window(String serviceName, Instant windowStart) {
        return encode(serviceName) + ":" + windowStart.toEpochMilli();
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
