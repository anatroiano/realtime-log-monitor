package com.example.log_collector_service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class LogCollectorServiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
