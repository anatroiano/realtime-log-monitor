package com.example.log_processor_service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {"spring.kafka.streams.auto-startup=false", "spring.kafka.admin.auto-create=false"})
class LogProcessorServiceApplicationTests {

	@Test
	void contextLoads() {
	}

}
