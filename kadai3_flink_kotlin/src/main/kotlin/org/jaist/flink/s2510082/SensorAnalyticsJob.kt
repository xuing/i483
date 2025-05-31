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
package org.jaist.flink.s2510082

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.regex.Pattern


/**
 * Flink Sensor Data Analytics Job
 * 
 * Reads sensor data stream from Kafka, calculates statistics (min, max, avg) for the last 5 minutes every 30 seconds
 * and publishes results back to Kafka
 */
object SensorAnalyticsJob {
    private val logger = LoggerFactory.getLogger(SensorAnalyticsJob::class.java)
    
    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("Starting Sensor Data Analytics Job")
        
        try {
            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            
            // Configure checkpointing
            env.enableCheckpointing(30000) // 30 second checkpoint
            logger.info("Checkpointing enabled with 30 second interval")
            
            // Configure event time and watermarks
            env.config.autoWatermarkInterval = 1000L
            logger.info("Watermark interval set to 1000ms")
            
            SensorAnalyticsProcessor(env).execute()
        } catch (e: Exception) {
            logger.error("Failed to execute Flink job", e)
            throw e
        }
    }
}

/**
 * Sensor Data Analytics Processor
 */
class SensorAnalyticsProcessor(private val env: StreamExecutionEnvironment) {

    companion object {
        private val logger = LoggerFactory.getLogger(SensorAnalyticsProcessor::class.java)
        
        // Configuration constants
        private const val KAFKA_BOOTSTRAP_SERVERS = "150.65.230.59:9092"
        private const val INPUT_TOPIC_PATTERN = "i483-sensors-s2510082-[A-Z0-9]+-[a-zA-Z0-9_]+"
        private const val OUTPUT_TOPIC_PREFIX = "i483-sensors-s2510082-analytics"
        private const val JOB_NAME = "Sensor Analytics Job"
        
        // Window configuration
        private val WINDOW_SIZE = Duration.ofMinutes(5) // 5-minute window
        private val SLIDE_SIZE = Duration.ofSeconds(30) // 30 second slide
    }

    fun execute() {
        logger.info("Setting up Kafka source with pattern: $INPUT_TOPIC_PATTERN")

        // 1. Create Kafka source
        val kafkaSource = createKafkaSource()

        // 2. Read raw data stream from Kafka
        val rawDataStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(), // No watermarks for raw data
            "sensor-data-source"
        )

        // 3. Parse raw data into SensorData objects and assign watermarks
        val sensorDataStream = rawDataStream
            .map { rawData ->
                logger.debug("Processing raw data: $rawData")
                SensorDataParser.parseSensorData(rawData)
            }
            .filter {
                val valid = it != null
                logger.debug("Filter result: {} for data: {}", valid, it)
                valid
            }
            .map {
                logger.debug("Valid sensor data: {}", it)
                it!!
            }
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .forBoundedOutOfOrderness<SensorData>(Duration.ofSeconds(10))
                    .withTimestampAssigner { element, _ ->
                        logger.debug("Assigning event time: {} to {}", element.timestamp, element)
                        element.timestamp
                    }
                    .withIdleness(Duration.ofSeconds(60)) // 防止source分区空闲时水印不前进
            )
        logger.info("Sensor data stream with watermarks assigned")

        // 4. Group by sensor type and measurement type, apply sliding window
        val analyticsStream = sensorDataStream
            .keyBy {
                val key = SensorKey(it.sensorName, it.dataType)
                logger.debug("Grouping data by key: {}, data: {}", key, it)
                key
            }
            .window(SlidingEventTimeWindows.of(WINDOW_SIZE, SLIDE_SIZE))
            .aggregate(
                SensorStatsAggregateFunction(),
                SensorStatsProcessWindowFunction()
            )

        // 5. Output to console (for debugging)
        analyticsStream.print("Analytics Results")

        // 6. 宿舍房间占用检测
        val occupancyDetector = OccupancyDetectionEventStream(analyticsStream)
        val occupancyEvents = occupancyDetector.detectOccupancyEvents()
        
        // 输出占用检测结果到控制台
        occupancyEvents.print("Occupancy Detection Results")

        // 5. Create Kafka sink for output
        val kafkaSink = createKafkaSink()
        analyticsStream
            .flatMap {stats, out: Collector<AnalyticsKafkaRecord> ->
                assert(stats.studentId == "s2510082") { "Student ID must be s2510082" }
                val baseTopic = "${OUTPUT_TOPIC_PREFIX}-${stats.sensorName}"
                out.collect(AnalyticsKafkaRecord("${baseTopic}_min-${stats.dataType}", stats.minValue.toString()))
                out.collect(AnalyticsKafkaRecord("${baseTopic}_max-${stats.dataType}", stats.maxValue.toString()))
                out.collect(AnalyticsKafkaRecord("${baseTopic}_avg-${stats.dataType}", stats.avgValue.toString()))
            }
            .returns(Types.POJO(AnalyticsKafkaRecord::class.java))
            .sinkTo(kafkaSink)
            .name("Kafka Analytics Sink")

        // 6. Execute job
        logger.info("Starting execution of $JOB_NAME")
        env.execute(JOB_NAME)
    }
    
    private fun createKafkaSource(): KafkaSource<String> {
        return KafkaSource.builder<String>()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopicPattern(Pattern.compile(INPUT_TOPIC_PATTERN))
            .setDeserializer(ConsumerRecordDeserializer())
            .setStartingOffsets(OffsetsInitializer.latest())
            .setGroupId("s2510082-sensor-flink-analytics-group")
            .build()
    }

    private fun createKafkaSink(): KafkaSink<AnalyticsKafkaRecord> {
        return KafkaSink.builder<AnalyticsKafkaRecord>()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<AnalyticsKafkaRecord>()
                    .setTopicSelector<AnalyticsKafkaRecord> { record ->
                        record.topic
                    }
                    .setValueSerializationSchema<AnalyticsKafkaRecord>{ record ->
                        record.value.toByteArray(Charsets.UTF_8)
                    }
                    .build()
            )
            .build()
    }

}

/**
 * Sensor statistics aggregation function
 */
class SensorStatsAggregateFunction : AggregateFunction<SensorData, StatsAccumulator, StatsAccumulator> {

    override fun createAccumulator(): StatsAccumulator {
        return StatsAccumulator()
    }
    
    override fun add(value: SensorData, accumulator: StatsAccumulator): StatsAccumulator {
        accumulator.sum += value.value
        accumulator.count++
        accumulator.min = minOf(accumulator.min, value.value)
        accumulator.max = maxOf(accumulator.max, value.value)
        return accumulator
    }
    
    override fun getResult(accumulator: StatsAccumulator): StatsAccumulator {
        return accumulator
    }
    
    override fun merge(a: StatsAccumulator, b: StatsAccumulator): StatsAccumulator {
        return StatsAccumulator(
            sum = a.sum + b.sum,
            count = a.count + b.count,
            min = minOf(a.min, b.min),
            max = maxOf(a.max, b.max)
        )
    }
}

/**
 * Sensor data parser
 */
object SensorDataParser {
    private val logger = LoggerFactory.getLogger(SensorDataParser::class.java)
    
    /**
     * Parse sensor data
     * Input format: "topic,timestamp,value"
     * Topic format: "i483-sensors-s2510082-SensorName-DataType"
     */
    fun parseSensorData(rawData: String): SensorData? {
        try {
            logger.debug("Parsing raw data: $rawData")
            val parts = rawData.split(",")
            if (parts.size < 3) {
                logger.warn("Invalid data format - insufficient parts: ${parts.size}")
                return null
            }
            
            val topic = parts[0]
            // Timestamp must exist
            val timestamp = parts[1].toLongOrNull()
            if (timestamp == null) {
                logger.warn("Invalid timestamp: ${parts[1]}")
                return null
            }
            
            val value = parts[2].toDoubleOrNull()
            if (value == null) {
                logger.warn("Invalid value: ${parts[2]}")
                return null
            }
            
            // Parse topic to extract sensor information
            // Format: i483-sensors-s2510082-SensorName-DataType
            val topicParts = topic.split("-")
            if (topicParts.size != 5 || topicParts[0] != "i483" || topicParts[1] != "sensors" || topicParts[2] != "s2510082") {
                logger.warn("Invalid topic format: $topic, parts: ${topicParts.size}")
                return null
            }
            
            val studentId = topicParts[2] // s2510082
            val sensorType = topicParts[3] // SensorType
            val dataType = topicParts[4] // DataType
            
            val sensorData = SensorData(
                topic = topic,
                timestamp = timestamp,
                sensorName = sensorType,
                dataType = dataType,
                value = value,
                studentId = studentId
            )

            logger.debug("Successfully parsed sensor data: {}", sensorData)
            return sensorData
        } catch (e: Exception) {
             logger.warn("Failed to parse sensor data: $rawData", e)
             return null
         }
     }

 }

/**
 * Sensor statistics window processing function
 */
class SensorStatsProcessWindowFunction : ProcessWindowFunction<StatsAccumulator, SensorStats, SensorKey, TimeWindow>() {
    private val logger = LoggerFactory.getLogger(SensorStatsProcessWindowFunction::class.java)

    override fun process(
        key: SensorKey,
        context: Context,
        elements: Iterable<StatsAccumulator>,
        out: Collector<SensorStats>
    ) {
        logger.debug(
            "Processing window for key: {}, window: {} - {}",
            key,
            context.window().start,
            context.window().end
        )
        
        val accumulator = elements.first()

        if (accumulator.count > 0) {
            // For 15 second intervals, 1-minute window, count should be around 4
            val expectedCount = (context.window().end - context.window().start) / 15000 // 15 seconds
            if (accumulator.count != expectedCount) {
                logger.info("Window processing - key: $key, actual count: ${accumulator.count}, expected: $expectedCount")
            }

            val stats = SensorStats(
                sensorName = key.sensorName,
                dataType = key.dataType,
                studentId = "s2510082",
                windowStart = context.window().start,
                windowEnd = context.window().end,
                minValue = accumulator.min,
                maxValue = accumulator.max,
                avgValue = accumulator.sum / accumulator.count,
                count = accumulator.count
            )

            logger.debug("Emitting stats: {}", stats)
            out.collect(stats)
        } else {
            logger.info("No data in window for key: {}", key)
        }
    }
}