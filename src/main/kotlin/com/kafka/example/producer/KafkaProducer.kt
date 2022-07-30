package com.kafka.example.producer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.kafka.example.CustomSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.DisposableBean
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate
import reactor.core.Disposable
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderResult

abstract class ReactiveKafkaProducer<T : Any>: InitializingBean, DisposableBean {

    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var bootstrapServers: String

    @Value("\${spring.kafka.producer.client-id}")
    lateinit var clientId: String

    abstract val topic: String

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

    open fun kafkaProducerProperties(): MutableMap<String, Any> =
        mutableMapOf(
            Pair(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
            Pair(ProducerConfig.CLIENT_ID_CONFIG, clientId),
        )

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