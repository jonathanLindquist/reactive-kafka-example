package com.kafka.example.config

import com.kafka.example.consumer.AbstractReactiveKafkaConsumer
import com.kafka.example.dto.BaseDTO
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.logging.Logger

@Configuration
class KafkaConsumerConfig {
    class BaseDTOConsumer: AbstractReactiveKafkaConsumer<BaseDTO>(BaseDTO::class.java) {
        private val logger = Logger.getLogger(this::class.simpleName)

        override val topics = listOf("example-reactive-topic")

        override suspend fun <T : Any> accept(dto: T): Boolean {
            logger.info("overridden accept method: ${this::class.simpleName}")
            return true
        }
    }

    @Bean
    fun baseDTOConsumer() = BaseDTOConsumer()
}