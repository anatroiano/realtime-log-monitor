package com.example.log_collector_service.http;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@OpenAPIDefinition(info = @Info(
        title = "Log Collector API",
        version = "v1",
        description = "API for receiving application log batches and publishing them to Kafka"
))
public class OpenApiConfiguration {
}
