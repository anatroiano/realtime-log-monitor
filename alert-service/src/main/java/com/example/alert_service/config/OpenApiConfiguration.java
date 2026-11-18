package com.example.alert_service.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@OpenAPIDefinition(info = @Info(
        title = "Alert API",
        version = "v1",
        description = "API for querying incidents triggered from aggregated application logs"
))
public class OpenApiConfiguration {
}
