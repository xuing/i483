/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jaist.flink.samplejob

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

/**
 * Flink DataStream Job for processing sensor data from Kafka.
 *
 * This application connects to Kafka topics matching the pattern 'i483-sensors-*'
 * and processes sensor data in real-time using Apache Flink.
 *
 * For a tutorial on writing Flink applications, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * To package this application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 */
object DataStreamJob {
    private val logger = LoggerFactory.getLogger(DataStreamJob::class.java)
    
    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("Starting Flink Sensor Data Processing Job")
        
        try {
            // Sets up the execution environment, which is the main entry point
            // to building Flink applications.
            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            
            // Configure environment for better performance
            env.enableCheckpointing(60000) // Checkpoint every 60 seconds
            
            SensorDataProcessor(env).execute()
        } catch (e: Exception) {
            logger.error("Failed to execute Flink job", e)
            throw e
        }
    }
}

/**
 * Main processor class for sensor data from Kafka topics.
 * 
 * This class sets up the Kafka source, processes the incoming sensor data,
 * and applies various transformations for real-time analytics.
 */
class SensorDataProcessor(private val env: StreamExecutionEnvironment) {
    
    companion object {
        private val logger = LoggerFactory.getLogger(SensorDataProcessor::class.java)
        
        // Configuration constants
        private const val KAFKA_BOOTSTRAP_SERVERS = "150.65.230.59:9092"
        private const val TOPIC_PATTERN = "i483-sensors-s2510082-[a-zA-Z0-9]+-[a-z_]+"
        private const val METADATA_MAX_AGE_MS = "5000"
        private const val JOB_NAME = "Sensor Data Processing Job"
    }
    
    fun execute() {
        logger.info("Setting up Kafka source with pattern: $TOPIC_PATTERN")
        
        // 1. Setup Kafka source with improved configuration
        val kafkaSource = createKafkaSource()
        
        // 2. Create data stream from Kafka source
        val sensorDataStream = env.fromSource(
            kafkaSource, 
            WatermarkStrategy.noWatermarks(), 
            "sensor-data-source"
        )

        // 3. Process the sensor data
        processSensorData(sensorDataStream)
        
        // 4. Execute the job
        logger.info("Starting execution of $JOB_NAME")
        env.execute(JOB_NAME)
    }
    
    private fun createKafkaSource(): KafkaSource<String> {
        return KafkaSource.builder<String>()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopicPattern(Pattern.compile(TOPIC_PATTERN))
            .setDeserializer(ConsumerRecordDeserializer())
            .setStartingOffsets(OffsetsInitializer.latest())
            .setProperty("properties.metadata.max.age.ms", METADATA_MAX_AGE_MS)
            .setProperty("auto.offset.reset", "latest")
            .setProperty("enable.auto.commit", "false")
            .build()
    }

    private fun processSensorData(dataStream: DataStream<String>) {
        // 假设 dataStream 是每行 "topic,timestamp,value"

    }
}
