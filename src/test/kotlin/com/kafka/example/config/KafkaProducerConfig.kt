package com.kafka.example.config

import com.kafka.example.dto.BaseDTO
import com.kafka.example.producer.AbstractReactiveKafkaProducer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.sender.SenderResult
import java.util.logging.Logger

@Configuration
class KafkaProducerConfig {
    class BaseDTOProducer : AbstractReactiveKafkaProducer<BaseDTO>() {
        private val logger = Logger.getLogger(this::class.simpleName)

        override val topic = "example-reactive-topic"

        override fun key() = "generated-key-${this::class.simpleName}-${this.uuid}"

        override suspend fun successHandler(senderResult: SenderResult<Void>) =
            logger.info("overridden success handler: ${this::class.simpleName}")

        override suspend fun errorHandler(throwable: Throwable) =
            logger.info("overridden error handler, error: ${throwable.message}")
    }

    @Bean
    fun baseDTOProducer() = BaseDTOProducer()
}
