package com.kafka.example.config

import com.kafka.example.KafkaServerTestProvider
import com.kafka.example.dto.BaseDTO
import com.kafka.example.producer.AbstractReactiveKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.sender.SenderResult
import java.util.UUID
import java.util.logging.Logger

@Configuration
class KafkaProducerConfig {
    class BaseDTOProducer: AbstractReactiveKafkaProducer<BaseDTO>() {
        private val logger = Logger.getLogger(this::class.simpleName)

        override val topic = "example-reactive-topic"

        override fun kafkaProducerProperties(): MutableMap<String, Any> =
            mutableMapOf(
                Pair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerTestProvider.KAFKA_CONTAINER.bootstrapServers),
                Pair(ProducerConfig.CLIENT_ID_CONFIG, "overridden-client-id-${UUID.randomUUID()}")
            )

        override fun generateKey() = "overridden-key-${this::class.simpleName}-${UUID.randomUUID()}"

        override fun successHandler(senderResult: SenderResult<Void>) = logger.info("overridden success handler: ${this::class.simpleName}")

        override fun errorHandler(throwable: Throwable) = logger.info("overridden error handler, error: ${throwable.message}")
    }

    @Bean
    fun baseDTOProducer() = BaseDTOProducer()
}