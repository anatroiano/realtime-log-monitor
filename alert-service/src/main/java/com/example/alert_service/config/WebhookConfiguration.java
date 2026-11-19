package com.example.alert_service.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(WebhookProperties.class)
public class WebhookConfiguration {

    @Bean
    RestClient webhookRestClient(WebhookProperties webhookProperties) {
        var httpClient = HttpClient.newBuilder()
                .connectTimeout(webhookProperties.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        var requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(webhookProperties.readTimeout());
        return RestClient.builder().requestFactory(requestFactory).build();
    }
}
