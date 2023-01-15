package com.kafka.example.producer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.example.CustomSerializer
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
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
import java.util.concurrent.Executors
import java.util.logging.Logger

abstract class AbstractReactiveKafkaProducer<T : Any> : InitializingBean, DisposableBean {

    companion object {
        private val dispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()
    }

    private val logger = Logger.getLogger(this::class.simpleName)

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.producer.client-id}")
    private lateinit var clientId: String

    private lateinit var producer: ReactiveKafkaProducerTemplate<String, T>

    protected val uuid: UUID = UUID.randomUUID()

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

    open fun key() = "${this::class.simpleName}-$uuid"

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

    open suspend fun send(dto: T): Disposable =
        coroutineScope {
            logger.info("Sending message: ${dto::class.simpleName}")
            return@coroutineScope producer
                .send(topic, key(), dto)
                .doOnNext { senderResult -> CoroutineScope(dispatcher).launch { successHandler(senderResult) } }
                .doOnError { throwable -> CoroutineScope(dispatcher).launch { errorHandler(throwable) } }
                .subscribe()
        }

    private val exceptionHandler: CoroutineExceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable ->
        logger.severe("${this::class.simpleName} exception handler error: ${throwable.message} | Context: $coroutineContext")
    }
}
