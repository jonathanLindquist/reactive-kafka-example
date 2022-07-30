package com.kafka.example.producer

import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.core.Disposable
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderResult

abstract class ReactiveKafkaProducer<T : Any>: InitializingBean, DisposableBean {

    abstract val topic: String

    private lateinit var producer: ReactiveKafkaProducerTemplate<String, T>

    override fun afterPropertiesSet() {
        producer = ReactiveKafkaProducerTemplate<String, T>(SenderOptions.create())
    }

    override fun destroy() {
        producer.close()
    }

    open fun successHandler(senderResult: SenderResult<Void>) {
        println("Sent item")
    }

    open fun errorHandler(throwable: Throwable) {
        println("Error sending item: ${throwable.message}")
    }

    open fun send(dto: T): Disposable =
        producer.send(topic, "1234", dto)
            .doOnNext { senderResult -> successHandler(senderResult) }
            .doOnError { throwable -> errorHandler(throwable) }
            .subscribe()
}