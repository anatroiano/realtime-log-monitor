package com.example.log_processor_service.kafka;

import com.example.log_processor_service.domain.LogEvent;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@RequiredArgsConstructor
final class LogRecordDeserializer implements Deserializer<DecodedLog> {
    private final JsonMapper jsonMapper;
    private final Validator validator;

    @Override
    public DecodedLog deserialize(String topic, byte[] data) {
        if (data == null) {
            return new DecodedLog(null, null, "MISSING_VALUE");
        }
        LogEvent event;
        try {
            event = jsonMapper.readValue(data, LogEvent.class);
        } catch (JacksonException exception) {
            return new DecodedLog(null, data, "INVALID_JSON");
        }
        if (event == null || !validator.validate(event).isEmpty()) {
            return new DecodedLog(null, data, "INVALID_FIELDS");
        }
        try {
            long timestamp = event.timestamp().toEpochMilli();
            if (timestamp < 0 || timestamp > Long.MAX_VALUE - ProcessorProperties.WINDOW_SIZE.toMillis()) {
                return new DecodedLog(null, data, "INVALID_TIMESTAMP");
            }
        } catch (ArithmeticException exception) {
            return new DecodedLog(null, data, "INVALID_TIMESTAMP");
        }
        return new DecodedLog(event, data, null);
    }
}
