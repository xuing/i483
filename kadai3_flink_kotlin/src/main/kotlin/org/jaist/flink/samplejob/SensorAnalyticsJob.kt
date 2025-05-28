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
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.regex.Pattern

/**
 * 传感器数据类
 */
data class SensorData(
    val topic: String,
    val timestamp: Long,
    val sensorType: String,
    val measurementType: String,
    val value: Double,
    val studentId: String
)

/**
 * 聚合统计结果类
 */
data class SensorStats(
    val sensorType: String,
    val measurementType: String,
    val studentId: String,
    val windowStart: Long,
    val windowEnd: Long,
    val minValue: Double,
    val maxValue: Double,
    val avgValue: Double,
    val count: Long
)

/**
 * 聚合累加器
 */
data class StatsAccumulator(
    var sum: Double = 0.0,
    var count: Long = 0,
    var min: Double = Double.MAX_VALUE,
    var max: Double = Double.MIN_VALUE
)

/**
 * Flink传感器数据分析作业
 * 
 * 从Kafka读取传感器数据流，每30秒计算最近5分钟数据的统计值（最小值、最大值、平均值）
 * 并将结果发布回Kafka
 */
object SensorAnalyticsJob {
    private val logger = LoggerFactory.getLogger(SensorAnalyticsJob::class.java)
    
    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("启动传感器数据分析作业")
        
        try {
            val env = StreamExecutionEnvironment.getExecutionEnvironment()
            
            // 配置检查点
            env.enableCheckpointing(30000) // 30秒检查点
            
            // 配置事件时间和水印
            env.config.autoWatermarkInterval = 1000L
            
            SensorAnalyticsProcessor(env).execute()
        } catch (e: Exception) {
            logger.error("执行Flink作业失败", e)
            throw e
        }
    }
}

/**
 * 传感器数据分析处理器
 */
class SensorAnalyticsProcessor(private val env: StreamExecutionEnvironment) {
    
    companion object {
        private val logger = LoggerFactory.getLogger(SensorAnalyticsProcessor::class.java)
        
        // 配置常量
        private const val KAFKA_BOOTSTRAP_SERVERS = "150.65.230.59:9092"
        private const val INPUT_TOPIC_PATTERN = "i483-sensors-s2510082-[a-zA-Z0-9]+-[a-z_]+"
        private const val OUTPUT_TOPIC_PREFIX = "i483-sensors-s2510082-analytics"
        private const val JOB_NAME = "Sensor Analytics Job"
        
        // 窗口配置
        private val WINDOW_SIZE = Duration.ofMinutes(5) // 5分钟窗口
        private val SLIDE_SIZE = Duration.ofSeconds(30) // 30秒滑动
    }
    
    fun execute() {
        logger.info("设置Kafka源，模式: $INPUT_TOPIC_PATTERN")
        
        // 1. 创建Kafka源
        val kafkaSource = createKafkaSource()
        
        // 2. 从Kafka源创建数据流
        val rawDataStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy
                .forBoundedOutOfOrderness<String>(Duration.ofSeconds(10))
                .withTimestampAssigner { element, _ ->
                    parseSensorData(element)?.timestamp ?: System.currentTimeMillis()
                },
            "sensor-data-source"
        )
        
        // 3. 解析和过滤传感器数据
        val sensorDataStream = rawDataStream
            .map { rawData -> SensorDataParser.parseSensorData(rawData) }
            .filter { it != null }
            .map { it!! }
        
        // 4. 按传感器类型和测量类型分组，应用滑动窗口
        val analyticsStream = sensorDataStream
            .keyBy { "${it.sensorType}-${it.measurementType}" }
            .window(SlidingEventTimeWindows.of(WINDOW_SIZE, SLIDE_SIZE))
            .aggregate(
                SensorStatsAggregateFunction(),
                SensorStatsProcessWindowFunction()
            )
        
        // 5. 输出到控制台（用于调试）
        analyticsStream.print("Analytics Results")
        
        // 6. 发送结果到Kafka
//        val kafkaSink = createKafkaSink()
//        analyticsStream
//            .map { stats -> formatOutputMessage(stats) }
//            .sinkTo(kafkaSink)
//
        // 7. 执行作业
        logger.info("开始执行 $JOB_NAME")
        env.execute(JOB_NAME)
    }
    
    private fun createKafkaSource(): KafkaSource<String> {
        return KafkaSource.builder<String>()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopicPattern(Pattern.compile(INPUT_TOPIC_PATTERN))
            .setDeserializer(ConsumerRecordDeserializer())
            .setStartingOffsets(OffsetsInitializer.latest())
            .setProperty("auto.offset.reset", "latest")
            .setProperty("enable.auto.commit", "false")
            .build()
    }
    
    private fun createKafkaSink(): KafkaSink<String> {
        return KafkaSink.builder<String>()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<String>()
                    .setTopicSelector<String> { record ->
                        // 从记录中提取topic名称
                        extractTopicFromRecord(record)
                    }
                    .setValueSerializationSchema(SimpleStringSchema())
                    .build()
            )
            .build()
    }
    
    /**
     * 解析传感器数据
     * 输入格式: "topic,timestamp,value"
     * topic格式: "i483-sensors-s2510082-SensorType-measurement_type"
     */
    private fun parseSensorData(rawData: String): SensorData? {
        try {
            val parts = rawData.split(",")
            if (parts.size < 3) return null
            
            val topic = parts[0]
            val timestamp = parts[1].toLongOrNull() ?: System.currentTimeMillis()
            val value = parts[2].toDoubleOrNull() ?: return null
            
            // 解析topic以提取传感器信息
            // 格式: i483-sensors-s2510082-SensorType-measurement_type
            val topicParts = topic.split("-")
            if (topicParts.size < 5) return null
            
            val studentId = topicParts[2] // s2510082
            val sensorType = topicParts[3] // SensorType
            val measurementType = topicParts[4] // measurement_type
            
            return SensorData(
                topic = topic,
                timestamp = timestamp,
                sensorType = sensorType,
                measurementType = measurementType,
                value = value,
                studentId = studentId
            )
        } catch (e: Exception) {
            logger.warn("解析传感器数据失败: $rawData", e)
            return null
        }
    }
    
    /**
     * 从输出记录中提取目标topic名称
     */
    private fun extractTopicFromRecord(record: String): String {
        try {
            // 记录格式包含传感器类型和测量类型信息
            val parts = record.split("|")
            if (parts.size >= 3) {
                val sensorType = parts[0]
                val measurementType = parts[1]
                val statType = parts[2] // min, max, avg
                return "$OUTPUT_TOPIC_PREFIX-$sensorType-$statType-$measurementType"
            }
        } catch (e: Exception) {
            logger.warn("提取topic名称失败: $record", e)
        }
        return "$OUTPUT_TOPIC_PREFIX-unknown"
    }
    
    /**
     * 格式化输出消息
     */
    private fun formatOutputMessage(stats: SensorStats): String {
        return "${stats.sensorType}|${stats.measurementType}|min|${stats.minValue}\n" +
               "${stats.sensorType}|${stats.measurementType}|max|${stats.maxValue}\n" +
               "${stats.sensorType}|${stats.measurementType}|avg|${stats.avgValue}"
    }
}

/**
 * 传感器统计聚合函数
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
 * 传感器统计窗口处理函数
 */
class SensorStatsProcessWindowFunction : ProcessWindowFunction<StatsAccumulator, SensorStats, String, TimeWindow>() {
    
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<StatsAccumulator>,
        out: Collector<SensorStats>
    ) {
        val accumulator = elements.first()
        
        if (accumulator.count > 0) {
            val keyParts = key.split("-")
            val sensorType = keyParts[0]
            val measurementType = keyParts[1]
            
            val stats = SensorStats(
                sensorType = sensorType,
                measurementType = measurementType,
                studentId = "s2510082",
                windowStart = context.window().start,
                windowEnd = context.window().end,
                minValue = accumulator.min,
                maxValue = accumulator.max,
                avgValue = accumulator.sum / accumulator.count,
                count = accumulator.count
            )
            
            out.collect(stats)
        }
    }
}