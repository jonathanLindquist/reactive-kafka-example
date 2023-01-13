package com.kafka.example

import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.logging.Logger

@ActiveProfiles("test")
@Testcontainers
object KafkaServerTestProvider {
    private val logger = Logger.getLogger(this::class.simpleName)

    @Container
    val KAFKA_CONTAINER: KafkaContainer = KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:latest")
    )

    class KafkaServerInitializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(applicationContext: ConfigurableApplicationContext) {
            KAFKA_CONTAINER.start()
            TestPropertyValues.of(
                "spring.kafka.bootstrap-servers=" + KAFKA_CONTAINER.bootstrapServers
            )
                .applyTo(applicationContext.environment)
            logger.info("Kafka for testing: ${KAFKA_CONTAINER.bootstrapServers}")
        }
    }
}