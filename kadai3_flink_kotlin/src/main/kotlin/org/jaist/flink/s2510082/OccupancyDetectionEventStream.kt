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
 * 创建占用状态Kafka输出
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
 * CO2递增检测器
 */
class CO2IncreaseDetector : ProcessWindowFunction<SensorData, Boolean, String, TimeWindow>() {
    
    companion object {
        private val logger = LoggerFactory.getLogger(CO2IncreaseDetector::class.java)
        private const val MIN_DATA_POINTS = 3 // 至少需要3个数据点才能判断趋势
    }
    
    override fun process(
        key: String,
        context: Context,
        elements: Iterable<SensorData>,
        out: Collector<Boolean>
    ) {
        // 按时间戳排序获取传感器数值序列
        val sortedData = elements.sortedBy { it.timestamp }
        val values = sortedData.map { it.value }
        
        // 至少需要3个数据点才能判断递增趋势
        if (values.size < MIN_DATA_POINTS) {
            return
        }
        
        // 检查是否递增（允许相等）
        val isIncreasing = values.zipWithNext().all { (a, b) -> a <= b }
        
        if (isIncreasing) {
            logger.info("检测到CO2持续上升 - 传感器: {}, 数据点: {}, 值范围: {}-{}", 
                key, values.size, values.first(), values.last())
            out.collect(true)
        }
    }
}

/**
 * 占用状态管理器
 */
class OccupancyStateManager : KeyedProcessFunction<String, Boolean, Boolean>() {
    
    companion object {
        private val logger = LoggerFactory.getLogger(OccupancyStateManager::class.java)
        private const val IDLE_TIMEOUT_MS = 60 * 60 * 1000L // 1小时超时
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
        // 只处理"有人"事件
        if (!value) return
        
        val wasOccupied = occState.value() ?: false
        
        // 状态变化：无人 -> 有人
        if (!wasOccupied) {
            logger.info("房间状态变更: 无人 -> 有人")
            occState.update(true)
            out.collect(true)
        }
        
        // 重置超时定时器
        resetIdleTimer(ctx)
    }
    
    private fun resetIdleTimer(ctx: Context) {
        // 删除旧定时器
        timerState.value()?.let { oldTimer ->
            ctx.timerService().deleteProcessingTimeTimer(oldTimer)
        }
        
        // 注册新定时器
        val newTimer = ctx.timerService().currentProcessingTime() + IDLE_TIMEOUT_MS
        ctx.timerService().registerProcessingTimeTimer(newTimer)
        timerState.update(newTimer)
    }
    
    override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<Boolean>) {
        val isOccupied = occState.value() ?: false
        
        if (isOccupied) {
            logger.info("房间状态变更: 有人 -> 无人 (超时: {}小时)", IDLE_TIMEOUT_MS / (60 * 60 * 1000))
            occState.update(false)
            out.collect(false)
        }
        
        // 清理定时器状态
        timerState.clear()
    }
}

/**
 * 宿舍房间占用检测事件流处理器
 */
class OccupancyDetectionEventStream(
    private val sensorDataStream: DataStream<SensorData>,
    private val sensorAnalyticsStream: DataStream<SensorStats>
) {
    
    companion object {
        private val logger = LoggerFactory.getLogger(OccupancyDetectionEventStream::class.java)
        
        // 检测阈值配置
        private const val LIGHT_SUDDEN_INCREASE_THRESHOLD = 100.0
        
        // 窗口配置
        private val CO2_WINDOW_SIZE = Duration.ofMinutes(5)
        private val CO2_SLIDE_SIZE = Duration.ofMinutes(1)
        private val LIGHT_DETECTION_WINDOW = Duration.ofMinutes(1)
    }
    
    /**
     * 主检测方法
     */
    fun detectOccupancyEvents(): DataStream<Boolean> {
        logger.info("启动宿舍房间占用检测系统")
        
        // 1. 检测开灯事件
        val lightingEvents = detectLightingEvents()
        
        // 2. 检测CO2上升事件
        val co2Events = detectCO2IncreaseEvents()
        
        // 3. 合并事件流并管理状态
        val occupancyStream = lightingEvents
            .union(co2Events)
            .keyBy { "global-occupancy" } // 全局状态管理
            .process(OccupancyStateManager())
            .name("Occupancy State Manager")
        
        // 4. 输出到Kafka
        occupancyStream
            .sinkTo(createOccupancyKafkaSink())
            .name("Kafka Occupancy Sink")
        
        return occupancyStream
    }
    
    /**
     * 检测开灯事件
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
                logger.info("检测到开灯事件 - 传感器: {}, 亮度变化: {}", 
                    lightEvent.sensorName, lightEvent.maxValue - lightEvent.minValue)
                true
            }
            .name("Lighting Event Detection")
    }
    
    /**
     * 检测CO2上升事件
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