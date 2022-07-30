package com.kafka.example

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

class CustomSerializer<T>(
    private val objectMapper: ObjectMapper
) : Serializer<T> {
    override fun serialize(topic: String?, data: T): ByteArray {
        return objectMapper.writeValueAsBytes(data)
    }
//    override fun deserialize(topic: String?, data: ByteArray?): T {
//        return try {
//            objectMapper.readValue(data, clazz)
//        } catch (e: Exception) {
//            println("mapping error: ${e.message}")
//            throw e
//        }
//    }
}