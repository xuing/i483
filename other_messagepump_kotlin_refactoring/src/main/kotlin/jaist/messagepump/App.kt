package jaist.messagepump

import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.util.*

/**
 * Main application entry point for the Message Pump.
 * This application forwards MQTT messages to Kafka topics.
 */
fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger("MessagePumpApp")
    
    try {
        logger.info("Starting Message Pump application...")
        
        // Load configuration properties
        val properties = Properties().apply {
            load(FileInputStream("config.properties"))
        }
        
        // Build and start the message pump
        val pump = MessagePumpConfig.fromProperties(properties).build()
        pump.start()
        
    } catch (e: FileNotFoundException) {
        logger.error("Configuration file 'config.properties' not found. Please create it based on config.properties.default", e)
        System.exit(1)
    } catch (e: IOException) {
        logger.error("Error reading configuration file", e)
        System.exit(1)
    } catch (e: Exception) {
        logger.error("Unexpected error occurred", e)
        System.exit(1)
    }
}