package org.jaist.flink.s2510082

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.v2.ValueStateDescriptor
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


private fun createOccupancyKafkaSink(): KafkaSink<Boolean> {
    return KafkaSink.builder<Boolean>()
        .setBootstrapServers("150.65.230.59:9092")
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
 * 宿舍房间占用检测事件流处理器
 * 使用Flink CEP检测以下占用模式：
 * 1. 开灯事件：亮度突然升高（而不是缓慢爬升）
 * 2. CO2持续增长或维持在高位
 */
class OccupancyDetectionEventStream(
    private val sensorDataStream: DataStream<SensorData>,
    private val sensorAnalyticsStream: DataStream<SensorStats>
) {

    companion object {
        private val logger = LoggerFactory.getLogger(OccupancyDetectionEventStream::class.java)

        // 检测阈值配置
        private const val LIGHT_SUDDEN_INCREASE_THRESHOLD = 100.0  // 亮度突然增加阈值
        // 1小时(毫秒)
        private const val OCCUPANCY_IDLE_TIMEOUT_MS = 60 * 60 * 1000L
    }

    /**
     * 主方法：检测占用状态（有人/无人）
     * - 只要检测到开灯事件或 CO₂ 持续上升事件，状态置为“有人”，并立即输出 true 写 Kafka；
     * - 如果 1 小时内未再收到“有人”事件，状态置为“无人”，并输出 false 写 Kafka。
     */
    fun detectOccupancyEvents(): DataStream<Boolean> {
        logger.info("开始设置宿舍房间占用检测")

        // 1. 亮度流过滤（只保留 dataType == "illumination" 的 SensorStats）
        val illuminationStream: DataStream<SensorStats> = sensorAnalyticsStream
            .filter { it.dataType.equals("illumination", ignoreCase = true) }

        // 2. 开灯事件：亮度突增
        val lightingEvents: DataStream<Boolean> = detectLightingEvents(illuminationStream)
            .map { true }  // 只要检测到事件就输出 true

        // 3. CO₂ 递增事件：检查过去 5 分钟窗口是否持续递增
        val co2IncreaseStream: DataStream<Boolean> = sensorDataStream
            .filter { it.dataType.equals("co2", ignoreCase = true) }
            .keyBy { it.sensorName }
            .window(SlidingEventTimeWindows.of(
                Duration.ofMinutes(5),
                Duration.ofMinutes(1)
            ))
            .process(object : ProcessWindowFunction<SensorData, Boolean, String, TimeWindow>() {
                override fun process(
                    key: String,
                    context: Context,
                    elements: Iterable<SensorData>,
                    out: Collector<Boolean>
                ) {
                    val values = elements.map { it.value }
                    val isIncreasing = values.zipWithNext().all { (a, b) -> a <= b }
                    if (isIncreasing) {
                        logger.info("检测到 CO2 持续上升事件（sensor={}，窗口内值={}）", key, values)
                        out.collect(true)
                    }
                }
            }).name("CO2 Increase Detection")

        // 4. 合并“开灯事件”与“CO₂ 递增事件”，进入状态机
        val occupancyEventStream: DataStream<Boolean> = lightingEvents
            .union(co2IncreaseStream)
            .keyBy { "occupancy_state" }  // 使用一个固定的 key 来聚合所有事件
            .process(object : KeyedProcessFunction<String, Boolean, Boolean>() {
                // 保存当前占用状态：true=有⼈, false=无人
                private val occStateDesc = ValueStateDescriptor<Boolean>("occState", Boolean::class.java)
                // 保存当前定时器的时间戳
                private val timerStateDesc = ValueStateDescriptor<Long>("idleTimer", Long::class.java)

                override fun processElement(
                    value: Boolean,
                    ctx: Context,
                    out: Collector<Boolean>
                ) {
                    val occState = runtimeContext.getState(occStateDesc)
                    val timerState = runtimeContext.getState(timerStateDesc)
                    val prev = occState.value() ?: false

                    // 如果之前是“无人”，现在收到 true → 状态切换为“有人”
                    if (!prev && value) {
                        logger.info("Occupancy State: Unoccupied → Occupied")
                        occState.update(true)
                        out.collect(true)  // 向下游（Kafka 等）输出“有人”
                    }
                    // 只要收到“有人”事件，无论之前状态如何，都要重置超时定时器
                    val prevTimerTs = timerState.value()
                    if (prevTimerTs != null) {
                        ctx.timerService().deleteProcessingTimeTimer(prevTimerTs)
                    }
                    val newTimerTs = ctx.timerService().currentProcessingTime() + OCCUPANCY_IDLE_TIMEOUT_MS
                    ctx.timerService().registerProcessingTimeTimer(newTimerTs)
                    timerState.update(newTimerTs)
                }

                override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<Boolean>) {
                    val occState = runtimeContext.getState(occStateDesc)
                    val prev = occState.value() ?: false
                    if (prev) {
                        logger.info("Occupancy State: Occupied → Unoccupied（超时 {} ms）", OCCUPANCY_IDLE_TIMEOUT_MS)
                        occState.update(false)
                        out.collect(false)  // 向下游输出“无人”
                    }
                    // 定时器只触发一次后，清除状态
                    runtimeContext.getState(timerStateDesc).clear()
                }
            }).name("Occupancy State Controller")

        // 5. 将状态变化流写入 Kafka（只输出 true/false）
        occupancyEventStream
            .sinkTo(createOccupancyKafkaSink())
            .name("Kafka Occupancy Detection Sink")

        // 6. 返回 occupancyEventStream，以便在外部还可以 print() 或做进一步逻辑
        return occupancyEventStream
    }


    /**
     * 检测开灯事件：亮度突然升高 illumination max - min > threshold
     */
    private fun detectLightingEvents(illuminationStream: DataStream<SensorStats>): DataStream<Boolean> {

        // 定义亮度突然升高的CEP模式
        val lightPattern = Pattern.begin<SensorStats>("light_on")
            .where(SimpleCondition.of { value: SensorStats ->
                (value.maxValue - value.minValue) > LIGHT_SUDDEN_INCREASE_THRESHOLD
            })
            .within(Duration.ofMinutes(1))  // 在1分钟内检测到的亮度变化

        // 应用CEP模式并提取匹配结果
        return CEP.pattern(illuminationStream, lightPattern).select {
            logger.info("检测到开灯事件: {}", it)
            true
        }
    }

}