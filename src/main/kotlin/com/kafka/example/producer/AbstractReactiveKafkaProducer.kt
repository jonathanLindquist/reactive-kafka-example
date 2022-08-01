package com.kafka.example.producer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.example.CustomSerializer
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.core.Disposable
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderResult
import java.util.UUID
import java.util.logging.Logger

abstract class AbstractReactiveKafkaProducer<T : Any> : InitializingBean, DisposableBean {

    private val logger = Logger.getLogger(this::class.simpleName)

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.producer.client-id}")
    private lateinit var clientId: String

    private lateinit var producer: ReactiveKafkaProducerTemplate<String, T>

    override fun afterPropertiesSet() {
        producer = ReactiveKafkaProducerTemplate(
            SenderOptions.create<String?, T>(kafkaProducerProperties())
                .withKeySerializer(StringSerializer())
                .withValueSerializer(CustomSerializer(jacksonObjectMapper()))
        )
    }

    override fun destroy() {
        producer.close()
    }

    abstract val topic: String

    open fun generateKey() = "${this::class.simpleName}-${UUID.randomUUID()}"

    open fun kafkaProducerProperties(): MutableMap<String, Any> =
        mutableMapOf(
            Pair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
            Pair(ProducerConfig.CLIENT_ID_CONFIG, clientId),
        )

    open suspend fun successHandler(senderResult: SenderResult<Void>) {
        logger.info("Sent item")
    }

    open suspend fun errorHandler(throwable: Throwable) {
        logger.severe("Error sending item: ${throwable.message}")
    }

    open suspend fun send(dto: T): Disposable = coroutineScope {
        logger.info("Sending message: ${dto::class.simpleName}")
        return@coroutineScope producer
            .send(topic, generateKey(), dto)
            .doOnNext { senderResult -> launch { successHandler(senderResult) } }
            .doOnError { throwable -> launch { errorHandler(throwable) } }
            .subscribe()
    }
}