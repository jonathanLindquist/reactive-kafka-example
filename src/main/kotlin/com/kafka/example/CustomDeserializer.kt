package com.kafka.example

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer

class CustomDeserializer<T>(
    private val objectMapper: ObjectMapper,
    private val clazz: Class<T>
) : Deserializer<T> {
    override fun deserialize(topic: String?, data: ByteArray?): T {
        return try {
            objectMapper.readValue(data, clazz)
        } catch (e: Exception) {
            println("mapping error: ${e.message}")
            throw e
        }
    }
}
