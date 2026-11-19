package com.example.alert_service.client;

import com.example.alert_service.config.WebhookProperties;
import com.example.alert_service.exception.WebhookDeliveryException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class WebhookClient {
    static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private final RestClient webhookRestClient;
    private final WebhookProperties webhookProperties;

    public void send(UUID alertId, String payload) {
        try {
            var response = webhookRestClient.post()
                    .uri(webhookProperties.url())
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(IDEMPOTENCY_KEY_HEADER, alertId.toString())
                    .body(payload)
                    .retrieve()
                    .toBodilessEntity();
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new WebhookDeliveryException("HTTP " + response.getStatusCode().value(), false, null);
            }
        } catch (RestClientResponseException exception) {
            throw new WebhookDeliveryException("HTTP " + exception.getStatusCode().value(),
                    isRetryable(exception.getStatusCode()), exception);
        } catch (RestClientException exception) {
            throw new WebhookDeliveryException(exception.getClass().getSimpleName(), true, exception);
        }
    }

    private boolean isRetryable(HttpStatusCode statusCode) {
        return statusCode.is5xxServerError()
                || statusCode.value() == HttpStatus.REQUEST_TIMEOUT.value()
                || statusCode.value() == HttpStatus.TOO_MANY_REQUESTS.value();
    }
}
