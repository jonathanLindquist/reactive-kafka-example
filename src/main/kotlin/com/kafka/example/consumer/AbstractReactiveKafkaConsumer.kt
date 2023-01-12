package com.kafka.example.consumer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.example.CustomDeserializer
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import reactor.kafka.receiver.ReceiverOptions
import java.util.logging.Logger

abstract class AbstractReactiveKafkaConsumer<T : Any>(
    private val clazz: Class<T>
) : InitializingBean, DisposableBean {

    private val logger = Logger.getLogger(this::class.simpleName)

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.consumer.group-id}")
    private lateinit var groupId: String

    @Value("\${spring.kafka.consumer.auto-offset-reset}")
    private lateinit var autoOffsetReset: String

    lateinit var receiver: ReactiveKafkaConsumerTemplate<String, T>

    lateinit var disposable: Job

    override fun afterPropertiesSet() {
        receiver = ReactiveKafkaConsumerTemplate(
            ReceiverOptions.create<String?, T>(kafkaConsumerProperties())
                .withKeyDeserializer(StringDeserializer())
                .withValueDeserializer(CustomDeserializer(jacksonObjectMapper(), clazz))
                .subscription(topics)
        )
        disposable = this.consume<T>()
    }

    override fun destroy() {
        disposable.cancel()
    }

    abstract val topics: List<String>

    open fun kafkaConsumerProperties(): MutableMap<String, Any> =
        mutableMapOf(
            Pair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
            Pair(ConsumerConfig.GROUP_ID_CONFIG, groupId),
            Pair(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset),
        )

    open suspend fun <T : Any> accept(dto: T) =
        logger.info("Found item: ${dto::class.simpleName}")

    open suspend fun errorHandler(throwable: Throwable) =
        logger.severe("Error: ${throwable.message}")

    @OptIn(DelicateCoroutinesApi::class)
    private fun <T> consume(): Job = GlobalScope.launch(exceptionHandler) {
        receiver
            .receiveAutoAck()
            .doOnNext { received ->
                logger.info(
                    """
                        |${this@AbstractReactiveKafkaConsumer::class.simpleName} received: 
                        |key=${received.key()}
                        |offset=${received.offset()}""".trimMargin()
                )
            }
            .doOnError { throwable -> launch { errorHandler(throwable) } }
            .subscribe { response -> launch { accept(response) } }
    }

    private val exceptionHandler: CoroutineExceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable ->
        logger.severe("${this::class.simpleName} exception handler error: ${throwable.message} | Context: $coroutineContext")
    }
}