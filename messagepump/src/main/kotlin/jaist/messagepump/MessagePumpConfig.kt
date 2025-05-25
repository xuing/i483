package jaist.messagepump

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.eclipse.paho.mqttv5.client.MqttClient
import org.eclipse.paho.mqttv5.common.MqttException
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Configuration class for the Message Pump application.
 * Handles MQTT and Kafka connection settings and creates configured instances.
 */
data class MessagePumpConfig(
    val mqttServerSpec: String,
    val mqttTopicsToSubscribe: Array<String>,
    val kafkaTopicSeparator: String = "-",
    val deadChannel: String = "i483-dead-messages",
    val kafkaProperties: Properties
) {
    
    companion object {
        private val logger = LoggerFactory.getLogger(MessagePumpConfig::class.java)
        
        // Configuration property keys
        const val MQTT_SERVER_SPEC = "mqtt.server"
        const val MQTT_TOPICS = "mqtt.topics.subscribe"
        const val KAFKA_TOPIC_SEPARATOR = "kafka.topic.separator"
        const val DEAD_CHANNEL = "kafka.deadchannel"
        const val BOOTSTRAP_SERVERS = "bootstrap.servers"
        
        /**
         * Creates a MessagePumpConfig from Properties object.
         * 
         * @param props Properties containing configuration values
         * @return Configured MessagePumpConfig instance
         * @throws IllegalArgumentException if required properties are missing
         */
        fun fromProperties(props: Properties): MessagePumpConfig {
            logger.info("Loading configuration from properties...")
            
            val mqttServerSpec = props.getRequiredProperty(MQTT_SERVER_SPEC)
            val mqttTopics = props.getRequiredProperty(MQTT_TOPICS)
                .split("\\s+".toRegex())
                .filter { it.isNotBlank() }
                .toTypedArray()
            
            val kafkaTopicSeparator = props.getProperty(KAFKA_TOPIC_SEPARATOR, "-")
            val deadChannel = props.getProperty(DEAD_CHANNEL, "i483-dead-messages")
            
            // Validate required Kafka properties
            props.getRequiredProperty(BOOTSTRAP_SERVERS)
            
            // Set default Kafka properties for optimal performance
            props.apply {
                putIfAbsent("linger.ms", "10")
                putIfAbsent("delivery.timeout.ms", "1000")
                putIfAbsent("request.timeout.ms", "500")
                putIfAbsent("partitioner.ignore.keys", "true")
                putIfAbsent("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                putIfAbsent("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                putIfAbsent("metadata.max.age.ms", "5000")
            }
            
            logger.info("Configuration loaded successfully. MQTT server: {}, Topics: {}", 
                mqttServerSpec, mqttTopics.joinToString(", "))
            
            return MessagePumpConfig(
                mqttServerSpec = mqttServerSpec,
                mqttTopicsToSubscribe = mqttTopics,
                kafkaTopicSeparator = kafkaTopicSeparator,
                deadChannel = deadChannel,
                kafkaProperties = props
            )
        }
        
        /**
         * Extension function to get a required property or throw an exception.
         */
        private fun Properties.getRequiredProperty(key: String): String {
            return getProperty(key) ?: throw IllegalArgumentException(
                "Required configuration property '$key' is missing or null"
            )
        }
    }
    
    /**
     * Builds and initializes a MessagePump instance with the current configuration.
     * 
     * @return Configured and initialized MessagePump
     * @throws MqttException if MQTT connection fails
     */
    @Throws(MqttException::class)
    fun build(): MessagePump {
        logger.info("Building MessagePump with configuration...")
        
        val mqttClient = MqttClient(mqttServerSpec, "messagepump")
        val kafkaProducer: Producer<String, String> = KafkaProducer(kafkaProperties)
        
        val pump = MessagePump(mqttClient, kafkaProducer, this)
        pump.initialize(mqttTopicsToSubscribe)
        
        logger.info("MessagePump built and initialized successfully")
        return pump
    }
    
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        
        other as MessagePumpConfig
        
        if (mqttServerSpec != other.mqttServerSpec) return false
        if (!mqttTopicsToSubscribe.contentEquals(other.mqttTopicsToSubscribe)) return false
        if (kafkaTopicSeparator != other.kafkaTopicSeparator) return false
        if (deadChannel != other.deadChannel) return false
        if (kafkaProperties != other.kafkaProperties) return false
        
        return true
    }
    
    override fun hashCode(): Int {
        var result = mqttServerSpec.hashCode()
        result = 31 * result + mqttTopicsToSubscribe.contentHashCode()
        result = 31 * result + kafkaTopicSeparator.hashCode()
        result = 31 * result + deadChannel.hashCode()
        result = 31 * result + kafkaProperties.hashCode()
        return result
    }
}