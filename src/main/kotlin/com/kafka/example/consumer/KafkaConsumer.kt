package com.kafka.example.consumer

import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate
import reactor.core.Disposable
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions

abstract class ReactiveKafkaConsumer<T>: InitializingBean, DisposableBean {

    abstract val topics: List<String>

    private lateinit var receiver: ReactiveKafkaConsumerTemplate<String, T>

    private lateinit var disposable: Disposable

    override fun afterPropertiesSet() {
        receiver = ReactiveKafkaConsumerTemplate<String, T>(
            ReceiverOptions.create()
        )
        disposable = this.consume<T>()
    }

    override fun destroy() {
        TODO("Not yet implemented")
    }

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