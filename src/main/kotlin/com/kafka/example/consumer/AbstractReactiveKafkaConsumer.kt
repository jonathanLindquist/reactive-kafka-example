package com.kafka.example.consumer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.example.CustomDeserializer
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import reactor.kafka.receiver.ReceiverOptions
import reactor.util.retry.Retry
import java.time.Duration
import java.util.concurrent.CancellationException
import java.util.concurrent.Executors
import java.util.logging.Logger

abstract class AbstractReactiveKafkaConsumer<T : Any>(
    private val clazz: Class<T>
) : InitializingBean, DisposableBean {

    companion object {
        private val dispatcher = Executors.newFixedThreadPool(4).asCoroutineDispatcher()
    }

    private val logger = Logger.getLogger(this::class.simpleName)

    @Value("\${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Value("\${spring.kafka.consumer.group-id}")
    private lateinit var groupId: String

    @Value("\${spring.kafka.consumer.auto-offset-reset}")
    private lateinit var autoOffsetReset: String

    @Value("\${spring.kafka.consumer.commit-batch-size:10}")
    private lateinit var commitBatchSize: Number

    lateinit var receiver: ReactiveKafkaConsumerTemplate<String, T>

    lateinit var disposable: Job

    override fun afterPropertiesSet() {
        receiver = ReactiveKafkaConsumerTemplate(
            ReceiverOptions.create<String?, T>(kafkaConsumerProperties()).withKeyDeserializer(StringDeserializer())
                .withValueDeserializer(CustomDeserializer(jacksonObjectMapper(), clazz))
                .commitBatchSize(commitBatchSize.toInt()).subscription(topics)
        )

        disposable = this.consume<T>()
    }

    override fun destroy() {
        disposable.cancel(
            CancellationException("App shutting down, cancelling ${this::class.simpleName} job")
        )
    }

    abstract val topics: List<String>

    open fun kafkaConsumerProperties(): MutableMap<String, Any> = mutableMapOf(
        Pair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
        Pair(ConsumerConfig.GROUP_ID_CONFIG, groupId),
        Pair(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset),
    )

    open suspend fun <T : Any> successHandler(dto: T): Boolean {
        return try {
            logger.info("Found item: ${dto::class.simpleName}")
            true
        } catch (e: Exception) {
            false
        }
    }

    open suspend fun onErrorContinueHandler(throwable: Throwable) =
        logger.warning("Warning, attempting to recover: ${throwable.message}")

    open suspend fun onErrorHandler(throwable: Throwable) = logger.severe("Error: ${throwable.message}")

    private fun <T> consume(): Job =
        CoroutineScope(dispatcher).launch(exceptionHandler) {
            receiver.receive()
                .doOnNext { response ->
                    logger.info(
                        """
                        |${this@AbstractReactiveKafkaConsumer::class.simpleName} response:
                        |key=${response.key()}
                        |offset=${response.offset()}""".trimMargin()
                    )
                    response.receiverOffset().acknowledge()
                }.onErrorContinue { throwable, response ->
                    // TODO: implement safe resumption, throw on DeadLetter queue
                    CoroutineScope(dispatcher).launch {
                        onErrorContinueHandler(throwable)
                    }
                }.doOnError { throwable ->
                    // safe close-out handling
                    CoroutineScope(dispatcher).launch {
                        onErrorHandler(throwable)
                    }
                }.retryWhen(
                    Retry.backoff(
                        3,
                        Duration.ofSeconds(2)
                    ).transientErrors(true)
                ).repeat()
                .subscribe { response ->
                    CoroutineScope(dispatcher).launch {
                        if (!successHandler(response))
                            true
                            // TODO: throw on DeadLetter queue if success handler fails
                    }
                }
        }

    private val exceptionHandler: CoroutineExceptionHandler = CoroutineExceptionHandler { coroutineContext, throwable ->
        logger.severe("${this::class.simpleName} exception handler error: ${throwable.message} | Context: $coroutineContext")
    }
}
