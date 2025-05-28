package org.jaist.flink.samplejob

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.slf4j.LoggerFactory

/**
 * Unit tests for the DataStreamJob and SensorDataProcessor classes.
 * 
 * These tests verify the basic functionality of the Flink job components
 * without requiring a full Kafka cluster setup.
 *
 * @author Updated for modern Flink and Kotlin
 */
class DataStreamJobTest {
    
    companion object {
        private val logger = LoggerFactory.getLogger(DataStreamJobTest::class.java)
        
        @JvmStatic
        @BeforeAll
        fun setUpClass() {
            logger.info("Setting up DataStreamJobTest class")
        }

        @JvmStatic
        @AfterAll
        fun tearDownClass() {
            logger.info("Tearing down DataStreamJobTest class")
        }
    }

    @BeforeEach
    fun setUp() {
        logger.debug("Setting up individual test")
    }

    @AfterEach
    fun tearDown() {
        logger.debug("Tearing down individual test")
    }

    /**
     * Test that the SensorDataProcessor can be instantiated with a local environment.
     * This test is disabled by default as it would try to connect to Kafka.
     */
    @Disabled("Requires Kafka connection - enable for integration testing")
    @Test
    fun testSensorDataProcessorInstantiation() {
        logger.info("Testing SensorDataProcessor instantiation")
        
        val env = StreamExecutionEnvironment.createLocalEnvironment()
        assertNotNull(env, "StreamExecutionEnvironment should not be null")
        
        val processor = SensorDataProcessor(env)
        assertNotNull(processor, "SensorDataProcessor should not be null")
        
        // Note: We don't call execute() here as it would try to connect to Kafka
        logger.info("SensorDataProcessor instantiation test completed successfully")
    }
    
    /**
     * Test that the StreamExecutionEnvironment can be created and configured.
     */
    @Test
    fun testEnvironmentSetup() {
        logger.info("Testing environment setup")
        
        val env = StreamExecutionEnvironment.createLocalEnvironment()
        assertNotNull(env, "StreamExecutionEnvironment should not be null")
        
        // Test that we can enable checkpointing
        assertDoesNotThrow {
            env.enableCheckpointing(60000)
        }
        
        logger.info("Environment setup test completed successfully")
    }
    
    /**
     * Test the ConsumerRecordDeserializer functionality.
     */
    @Test
    fun testConsumerRecordDeserializer() {
        logger.info("Testing ConsumerRecordDeserializer")
        
        val deserializer = ConsumerRecordDeserializer()
        assertNotNull(deserializer, "ConsumerRecordDeserializer should not be null")
        
        val typeInfo = deserializer.producedType
        assertNotNull(typeInfo, "Produced type should not be null")
        assertEquals("String", typeInfo.toString(), "Produced type should be String")
        
        logger.info("ConsumerRecordDeserializer test completed successfully")
    }
}
