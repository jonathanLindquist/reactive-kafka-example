package com.kafka.example

import com.kafka.example.config.KafkaConsumerConfig
import com.kafka.example.config.KafkaProducerConfig
import com.kafka.example.dto.BaseDTO
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.junit.jupiter.Testcontainers

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ContextConfiguration(
    initializers = [KafkaServerTestProvider.KafkaServerInitializer::class],
    classes = [ExampleApplication::class, KafkaConsumerConfig::class, KafkaProducerConfig::class]
)
class ExampleApplicationTests {

    @Autowired
    lateinit var baseDTOProducer: KafkaProducerConfig.BaseDTOProducer

    @Test
    fun `producer sends & receiver consumes`(): Unit = runBlocking {
        CoroutineScope(Dispatchers.IO).launch {
            baseDTOProducer.send(
                BaseDTO(
                    item = "first",
                    amount = 1
                )
            )
        }

        CoroutineScope(Dispatchers.IO).launch {
            delay(1000L)
            baseDTOProducer.send(
                BaseDTO(
                    item = "second",
                    amount = 2
                )
            )
        }

        CoroutineScope(Dispatchers.IO).launch {
            delay(1000L)
            baseDTOProducer.send(
                BaseDTO(
                    item = "third",
                    amount = 3
                )
            )
        }

        // wait for the consumer to receive, can be verified in the log output
        launch {
            delay(5000)
        }.join()
    }
}
