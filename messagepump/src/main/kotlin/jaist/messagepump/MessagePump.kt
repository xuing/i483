package jaist.messagepump

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.eclipse.paho.mqttv5.client.*
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage
import org.eclipse.paho.mqttv5.common.packet.MqttProperties
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Represents a message received from MQTT that needs to be forwarded to Kafka.
 */
data class MqttMessageData(
    val topic: String,
    val payload: String
)

/**
 * Main message pump class that forwards MQTT messages to Kafka.
 * Uses Kotlin coroutines for efficient asynchronous processing.
 */
class MessagePump(
    private val mqttClient: MqttClient,
    private val kafkaProducer: Producer<String, String>,
    private val config: MessagePumpConfig
) : MqttCallback {
    
    companion object {
        private val logger = LoggerFactory.getLogger(MessagePump::class.java)
        private const val MESSAGE_CHANNEL_CAPACITY = 1024
    }
    
    private val messageChannel = Channel<MqttMessageData>(MESSAGE_CHANNEL_CAPACITY)
    private val isRunning = AtomicBoolean(false)
    private var processingJob: Job? = null
    
    /**
     * Initializes the MQTT client and subscribes to configured topics.
     * 
     * @param topics Array of MQTT topics to subscribe to
     * @throws MqttException if connection or subscription fails
     */
    @Throws(MqttException::class)
    fun initialize(topics: Array<String>) {
        logger.info("Initializing MessagePump with topics: {}", topics.joinToString(", "))
        
        mqttClient.setCallback(this)
        mqttClient.connect()
        
        // Subscribe to all topics with QoS 0 for simplicity
        val qosLevels = IntArray(topics.size) { 0 }
        mqttClient.subscribe(topics, qosLevels)
        
        logger.info("Successfully connected to MQTT broker and subscribed to topics")
    }
    
    /**
     * Starts the message pump processing loop.
     * This method blocks until the pump is stopped.
     */
    fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("MessagePump is already running")
            return
        }
        
        logger.info("Starting MessagePump...")
        
        runBlocking {
            processingJob = launch {
                processMessages()
            }
            
            try {
                processingJob?.join()
            } catch (e: Exception) {
                logger.error("Error in message processing", e)
            } finally {
                cleanup()
            }
        }
    }
    
    /**
     * Stops the message pump gracefully.
     */
    fun stop() {
        logger.info("Stopping MessagePump...")
        isRunning.set(false)
        processingJob?.cancel()
        messageChannel.close()
    }
    
    /**
     * Processes messages from the channel and forwards them to Kafka.
     */
    private suspend fun processMessages() {
        logger.info("Started message processing coroutine")
        
        try {
            for (message in messageChannel) {
                if (!isRunning.get()) break
                
                try {
                    forwardToKafka(message)
                } catch (e: Exception) {
                    logger.error("Error forwarding message to Kafka", e)
                    handleDeadMessage(message, e)
                }
            }
        } catch (e: Exception) {
            logger.error("Error in message processing loop", e)
        }
        
        logger.info("Message processing coroutine finished")
    }
    
    /**
     * Forwards a message to the appropriate Kafka topic.
     */
    private suspend fun forwardToKafka(message: MqttMessageData) {
        val kafkaTopic = escapeMqttTopic(message.topic)
        
        logger.debug("Forwarding message: topic={}, payload={}", kafkaTopic, message.payload)
        
        withContext(Dispatchers.IO) {
            val record = ProducerRecord<String, String>(kafkaTopic, message.payload)
            kafkaProducer.send(record) { metadata, exception ->
                if (exception != null) {
                    logger.error("Failed to send message to Kafka topic: {}", kafkaTopic, exception)
                } else {
                    logger.debug("Message sent successfully to Kafka topic: {} at offset: {}", 
                        kafkaTopic, metadata?.offset())
                }
            }
        }
    }
    
    /**
     * Handles messages that couldn't be processed successfully.
     */
    private suspend fun handleDeadMessage(message: MqttMessageData, error: Exception) {
        try {
            val deadMessagePayload = """{
                "error": "${error.message?.replace("\"", "\\\"")}",
                "originalTopic": "${message.topic}",
                "originalPayload": "${message.payload.replace("\"", "\\\"")}",
                "timestamp": "${System.currentTimeMillis()}"
            }""".trimIndent()
            
            withContext(Dispatchers.IO) {
                val deadRecord = ProducerRecord<String, String>(config.deadChannel, deadMessagePayload)
                kafkaProducer.send(deadRecord)
            }
            
            logger.warn("Sent dead message to channel: {}", config.deadChannel)
        } catch (e: Exception) {
            logger.error("Failed to send message to dead channel", e)
        }
    }
    
    /**
     * Converts MQTT topic format to Kafka-compatible topic name.
     */
    private fun escapeMqttTopic(mqttTopic: String): String {
        return mqttTopic.replace("/", config.kafkaTopicSeparator)
    }
    
    /**
     * Cleans up resources when stopping the pump.
     */
    private fun cleanup() {
        logger.info("Cleaning up MessagePump resources...")
        
        try {
            kafkaProducer.close()
            logger.info("Kafka producer closed")
        } catch (e: Exception) {
            logger.error("Error closing Kafka producer", e)
        }
        
        try {
            if (mqttClient.isConnected) {
                mqttClient.close()
            }
            logger.info("MQTT client closed")
        } catch (e: Exception) {
            logger.error("Error closing MQTT client", e)
        }
    }
    
    // MQTT Callback implementations
    
    override fun disconnected(disconnectResponse: MqttDisconnectResponse?) {
        logger.warn("MQTT client disconnected: {}", disconnectResponse?.reasonString ?: "Unknown reason")
    }
    
    override fun mqttErrorOccurred(exception: MqttException?) {
        logger.error("MQTT error occurred", exception)
    }
    
    override fun messageArrived(topic: String, message: MqttMessage) {
        val payloadString = String(message.payload)
        logger.debug("MQTT message arrived - Topic: {}, Payload: {}", topic, payloadString)
        
        val mqttMessage = MqttMessageData(topic, payloadString)
        
        // Try to send to channel, drop message if channel is full
        if (!messageChannel.trySend(mqttMessage).isSuccess) {
            logger.warn("Message channel is full, dropping message from topic: {}", topic)
        }
    }
    
    override fun deliveryComplete(token: IMqttToken?) {
        logger.debug("MQTT delivery complete for token: {}", token?.messageId)
    }
    
    override fun connectComplete(reconnect: Boolean, serverURI: String?) {
        logger.info("MQTT connection complete - Server: {}, Reconnect: {}", serverURI, reconnect)
    }
    
    override fun authPacketArrived(reasonCode: Int, properties: MqttProperties?) {
        logger.debug("MQTT auth packet arrived - Reason code: {}", reasonCode)
    }
}