package jaist.messagepump

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Unit tests for MessagePumpConfig class.
 */
class MessagePumpConfigTest {
    
    @Test
    fun `should create config from valid properties`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.server", "tcp://localhost:1883")
            setProperty("mqtt.topics.subscribe", "topic1 topic2 topic3")
            setProperty("kafka.topic.separator", "_")
            setProperty("kafka.deadchannel", "test-dead")
            setProperty("bootstrap.servers", "localhost:9092")
        }
        
        // When
        val config = MessagePumpConfig.fromProperties(properties)
        
        // Then
        assertEquals("tcp://localhost:1883", config.mqttServerSpec)
        assertEquals(3, config.mqttTopicsToSubscribe.size)
        assertTrue(config.mqttTopicsToSubscribe.contains("topic1"))
        assertTrue(config.mqttTopicsToSubscribe.contains("topic2"))
        assertTrue(config.mqttTopicsToSubscribe.contains("topic3"))
        assertEquals("_", config.kafkaTopicSeparator)
        assertEquals("test-dead", config.deadChannel)
        assertNotNull(config.kafkaProperties)
    }
    
    @Test
    fun `should use default values when optional properties are missing`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.server", "tcp://localhost:1883")
            setProperty("mqtt.topics.subscribe", "topic1")
            setProperty("bootstrap.servers", "localhost:9092")
        }
        
        // When
        val config = MessagePumpConfig.fromProperties(properties)
        
        // Then
        assertEquals("-", config.kafkaTopicSeparator)
        assertEquals("i483-dead-messages", config.deadChannel)
    }
    
    @Test
    fun `should throw exception when required mqtt server property is missing`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.topics.subscribe", "topic1")
            setProperty("bootstrap.servers", "localhost:9092")
        }
        
        // When & Then
        assertThrows<IllegalArgumentException> {
            MessagePumpConfig.fromProperties(properties)
        }
    }
    
    @Test
    fun `should throw exception when required mqtt topics property is missing`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.server", "tcp://localhost:1883")
            setProperty("bootstrap.servers", "localhost:9092")
        }
        
        // When & Then
        assertThrows<IllegalArgumentException> {
            MessagePumpConfig.fromProperties(properties)
        }
    }
    
    @Test
    fun `should throw exception when required bootstrap servers property is missing`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.server", "tcp://localhost:1883")
            setProperty("mqtt.topics.subscribe", "topic1")
        }
        
        // When & Then
        assertThrows<IllegalArgumentException> {
            MessagePumpConfig.fromProperties(properties)
        }
    }
    
    @Test
    fun `should handle topics with multiple whitespace separators`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.server", "tcp://localhost:1883")
            setProperty("mqtt.topics.subscribe", "topic1   topic2\ttopic3\n\ttopic4")
            setProperty("bootstrap.servers", "localhost:9092")
        }
        
        // When
        val config = MessagePumpConfig.fromProperties(properties)
        
        // Then
        assertEquals(4, config.mqttTopicsToSubscribe.size)
        assertTrue(config.mqttTopicsToSubscribe.contains("topic1"))
        assertTrue(config.mqttTopicsToSubscribe.contains("topic2"))
        assertTrue(config.mqttTopicsToSubscribe.contains("topic3"))
        assertTrue(config.mqttTopicsToSubscribe.contains("topic4"))
    }
    
    @Test
    fun `should set default kafka properties`() {
        // Given
        val properties = Properties().apply {
            setProperty("mqtt.server", "tcp://localhost:1883")
            setProperty("mqtt.topics.subscribe", "topic1")
            setProperty("bootstrap.servers", "localhost:9092")
        }
        
        // When
        val config = MessagePumpConfig.fromProperties(properties)
        
        // Then
        val kafkaProps = config.kafkaProperties
        assertEquals("10", kafkaProps.getProperty("linger.ms"))
        assertEquals("1000", kafkaProps.getProperty("delivery.timeout.ms"))
        assertEquals("500", kafkaProps.getProperty("request.timeout.ms"))
        assertEquals("true", kafkaProps.getProperty("partitioner.ignore.keys"))
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", 
            kafkaProps.getProperty("key.serializer"))
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", 
            kafkaProps.getProperty("value.serializer"))
        assertEquals("5000", kafkaProps.getProperty("metadata.max.age.ms"))
    }
}