package com.kafka.example

import com.kafka.example.config.KafkaConsumerConfig
import com.kafka.example.config.KafkaProducerConfig
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.junit.jupiter.Testcontainers

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ContextConfiguration(
	initializers = [KafkaServerTestProvider.KafkaServerInitializer::class],
	classes = [ExampleApplication::class, KafkaConsumerConfig::class, KafkaProducerConfig::class]
)
class ExampleApplicationTests {

	@Test
	fun contextLoads() {
	}

}
