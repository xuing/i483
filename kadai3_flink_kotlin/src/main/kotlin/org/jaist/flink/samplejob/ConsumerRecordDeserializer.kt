package org.jaist.flink.samplejob

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

/**
 * Custom deserializer for Kafka ConsumerRecord that extracts structured information
 * including topic, timestamp, partition, offset, and value.
 * 
 * This deserializer provides enhanced metadata extraction compared to simple string deserialization.
 */
class ConsumerRecordDeserializer : KafkaRecordDeserializationSchema<String> {
    
    companion object {
        private val logger = LoggerFactory.getLogger(ConsumerRecordDeserializer::class.java)
    }
    
    override fun deserialize(record: ConsumerRecord<ByteArray, ByteArray>, out: Collector<String>) {
        try {
            val value = record.value()?.let { String(it, Charsets.UTF_8) } ?: "null"
            val structuredData = "${record.topic()},${record.timestamp()},$value"

            out.collect(structuredData)
        } catch (e: Exception) {
            logger.error("Error deserializing record from topic: {}, partition: {}, offset: {}, error: {}", 
                record.topic(), record.partition(), record.offset(), e.message, e)
            out.collect("ERROR: ${e.message}")
        }
    }
    
    override fun getProducedType(): TypeInformation<String> {
        return TypeInformation.of(String::class.java)
    }
}
