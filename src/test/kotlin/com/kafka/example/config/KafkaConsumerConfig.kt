package com.kafka.example.config

import com.kafka.example.KafkaServerTestProvider
import com.kafka.example.consumer.AbstractReactiveKafkaConsumer
import com.kafka.example.dto.BaseDTO
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.UUID
import java.util.logging.Logger

@Configuration
class KafkaConsumerConfig {
    class BaseDTOConsumer: AbstractReactiveKafkaConsumer<BaseDTO>(BaseDTO::class.java) {
        private val logger = Logger.getLogger(this::class.simpleName)

        override val topics = listOf("example-reactive-topic")

        override fun kafkaConsumerProperties(): MutableMap<String, Any> =
            mutableMapOf(
                Pair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerTestProvider.KAFKA_CONTAINER.bootstrapServers),
                Pair(ConsumerConfig.GROUP_ID_CONFIG, "overridden-group-id-${UUID.randomUUID()}"),
                Pair(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
            )

        override suspend fun <T : Any> accept(dto: T) = logger.info("overridden accept method: ${this::class.simpleName}")
    }

    @Bean
    fun baseDTOConsumer() = BaseDTOConsumer()
}