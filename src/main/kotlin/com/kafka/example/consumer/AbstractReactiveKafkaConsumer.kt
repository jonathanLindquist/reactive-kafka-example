package com.kafka.example.consumer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.example.CustomDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import reactor.core.Disposable
import reactor.kafka.receiver.ReceiverOptions

abstract class AbstractReactiveKafkaConsumer<T>(
    private val clazz: Class<T>
): InitializingBean, DisposableBean {

    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var bootstrapServers: String

    @Value("\${spring.kafka.consumer.group-id}")
    lateinit var groupId: String

    @Value("\${spring.kafka.consumer.auto-offset-reset}")
    lateinit var autoOffsetReset: String

    abstract val topics: List<String>

    private lateinit var receiver: ReactiveKafkaConsumerTemplate<String, T>

    private lateinit var disposable: Disposable

    override fun afterPropertiesSet() {
        receiver = ReactiveKafkaConsumerTemplate<String, T>(
            ReceiverOptions.create<String?, T>(kafkaConsumerProperties())
                .withKeyDeserializer(StringDeserializer())
                .withValueDeserializer(CustomDeserializer(jacksonObjectMapper(), clazz))
                .subscription(topics)
        )
        disposable = this.consume<T>()
    }

    override fun destroy() {
        TODO("Not yet implemented")
    }

    open fun kafkaConsumerProperties(): MutableMap<String, Any> =
        mutableMapOf(
            Pair(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
            Pair(ConsumerConfig.GROUP_ID_CONFIG, groupId),
            Pair(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset),
        )

    open fun <T> accept(dto: T) =
        println("Found item: ${dto!!::class.simpleName}")

    private fun <T> consume(): Disposable =
        receiver
            .receiveAutoAck()
            .doOnNext { received -> println(
                """logging item received: 
                    |key=${received.key()}
                    |offset=${received.offset()}""".trimMargin()
            ) }
            .doOnError { throwable -> println("Error: ${throwable.message}") }
            .subscribe { response -> accept(response) }
}