package org.jaist.flink.s2510082

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.cep.CEP
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Create Kafka sink for occupancy state output
 */
private fun createOccupancyKafkaSink(): KafkaSink<Boolean> {
    return KafkaSink.builder<Boolean>()
        .setBootstrapServers("150.65.230.59:9092")
        .setTransactionalIdPrefix("occupancy-sink")
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder<Boolean>()
                .setTopic("i483-sensors-s2510082-analytics-occupancy_detection")
                .setValueSerializationSchema(object : SerializationSchema<Boolean> {
                    override fun serialize(element: Boolean): ByteArray {
                        val status = if (element) "Occupied" else "Unoccupied"
                        return status.toByteArray(Charsets.UTF_8)
                    }
                })
                .build()
        )
        .build()
}

/**
 * CO2 increase detector
 */
class CO2IncreaseDetector : ProcessWindowFunction<SensorData, Boolean, String, TimeWindow>() {
    
    companion object {
        private val logger = LoggerFactory.getLogger(CO2IncreaseDetector::class.java)
        private const val MIN_DATA_POINTS = 3 // Minimum 3 data points required to determine trend
    }
    
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SensorData>,
        out: Collector<Boolean>
    ) {
        // Sort by timestamp to get sensor value sequence
        val sortedData = elements.sortedBy { it.timestamp }
        val values = sortedData.map { it.value }
        
        // At least 3 data points required to determine increasing trend
        if (values.size < MIN_DATA_POINTS) {
            return
        }
        
        // Check if values are increasing (equal values allowed)
        val isIncreasing = values.zipWithNext().all { (a, b) -> a <= b }
        
        if (isIncreasing) {
            logger.info("CO2 continuous increase detected - Sensor: {}, Data points: {}, Value range: {}-{}", 
                key, values.size, values.first(), values.last())
            out.collect(true)
        }
    }
}

/**
 * Occupancy state manager
 */
class OccupancyStateManager : KeyedProcessFunction<String, Boolean, Boolean>() {
    
    companion object {
        private val logger = LoggerFactory.getLogger(OccupancyStateManager::class.java)
        private const val IDLE_TIMEOUT_MS = 60 * 60 * 1000L // 1 hour timeout
    }
    
    private lateinit var occState: ValueState<Boolean>
    private lateinit var timerState: ValueState<Long>
    
    override fun open(openContext: OpenContext) {
        super.open(openContext)
        occState = runtimeContext.getState(
            ValueStateDescriptor("occupancy-state", Boolean::class.java)
        )
        timerState = runtimeContext.getState(
            ValueStateDescriptor("idle-timer", Long::class.java)
        )
    }
    
    override fun processElement(
        value: Boolean,
        ctx: Context,
        out: Collector<Boolean>
    ) {
        // Only process "occupied" events
        if (!value) return
        
        val wasOccupied = occState.value() ?: false
        
        // State change: unoccupied -> occupied
        if (!wasOccupied) {
            logger.info("Room state changed: unoccupied -> occupied")
            occState.update(true)
            out.collect(true)
        }
        
        // Reset idle timeout timer
        resetIdleTimer(ctx)
    }
    
    private fun resetIdleTimer(ctx: Context) {
        // Delete old timer
        timerState.value()?.let { oldTimer ->
            ctx.timerService().deleteProcessingTimeTimer(oldTimer)
        }
        
        // Register new timer
        val newTimer = ctx.timerService().currentProcessingTime() + IDLE_TIMEOUT_MS
        ctx.timerService().registerProcessingTimeTimer(newTimer)
        timerState.update(newTimer)
    }
    
    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<Boolean>) {
        val isOccupied = occState.value() ?: false
        
        if (isOccupied) {
            logger.info("Room state changed: occupied -> unoccupied (timeout: {} hours)", IDLE_TIMEOUT_MS / (60 * 60 * 1000))
            occState.update(false)
            out.collect(false)
        }
        
        // Clear timer state
        timerState.clear()
    }
}

/**
 * Dormitory room occupancy detection event stream processor
 */
class OccupancyDetectionEventStream(
    private val sensorDataStream: DataStream<SensorData>,
    private val sensorAnalyticsStream: DataStream<SensorStats>
) {
    
    companion object {
        private val logger = LoggerFactory.getLogger(OccupancyDetectionEventStream::class.java)
        
        // Detection threshold configuration
        private const val LIGHT_SUDDEN_INCREASE_THRESHOLD = 100.0
        
        // Window configuration
        private val CO2_WINDOW_SIZE = Duration.ofMinutes(5)
        private val CO2_SLIDE_SIZE = Duration.ofMinutes(1)
        private val LIGHT_DETECTION_WINDOW = Duration.ofMinutes(1)
    }
    
    /**
     * Main detection method
     */
    fun detectOccupancyEvents(): DataStream<Boolean> {
        logger.info("Starting dormitory room occupancy detection system")
        
        // 1. Detect lighting events
        val lightingEvents = detectLightingEvents()
        
        // 2. Detect CO2 increase events
        val co2Events = detectCO2IncreaseEvents()
        
        // 3. Merge event streams and manage state
        val occupancyStream = lightingEvents
            .union(co2Events)
            .keyBy { "global-occupancy" } // Global state management
            .process(OccupancyStateManager())
            .name("Occupancy State Manager")
        
        // 4. Output to Kafka
        occupancyStream
            .sinkTo(createOccupancyKafkaSink())
            .name("Kafka Occupancy Sink")
        
        return occupancyStream
    }
    
    /**
     * Detect lighting events
     */
    private fun detectLightingEvents(): DataStream<Boolean> {
        val illuminationStream = sensorAnalyticsStream
            .filter { it.dataType.equals("illumination", ignoreCase = true) }
        
        val lightPattern = Pattern.begin<SensorStats>("light_on")
            .where(SimpleCondition.of { stats ->
                (stats.maxValue - stats.minValue) > LIGHT_SUDDEN_INCREASE_THRESHOLD
            })
            .within(LIGHT_DETECTION_WINDOW)
        
        return CEP.pattern(illuminationStream, lightPattern)
            .select { matchedEvents ->
                val lightEvent = matchedEvents["light_on"]!![0]
                logger.info("Lighting event detected - Sensor: {}, Brightness change: {}", 
                    lightEvent.sensorName, lightEvent.maxValue - lightEvent.minValue)
                true
            }
            .name("Lighting Event Detection")
    }
    
    /**
     * Detect CO2 increase events
     */
    private fun detectCO2IncreaseEvents(): DataStream<Boolean> {
        return sensorDataStream
            .filter { it.dataType.equals("co2", ignoreCase = true) }
            .keyBy { it.sensorName }
            .window(SlidingEventTimeWindows.of(CO2_WINDOW_SIZE, CO2_SLIDE_SIZE))
            .process(CO2IncreaseDetector())
            .name("CO2 Increase Detection")
    }
}