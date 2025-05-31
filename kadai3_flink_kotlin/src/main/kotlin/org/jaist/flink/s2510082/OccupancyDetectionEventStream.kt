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
     * 处理传感器统计流以检测占用事件
     * 应用CEP模式匹配来识别基于传感器数据的占用事件
     *
     * @return 满足占用检测条件的SensorStats数据流
     */
    fun detectOccupancyEvents(): DataStream<Boolean> {
        logger.info("开始设置宿舍房间占用检测")

        // 1. 拆分原始流
        val illuminationStream = sensorAnalyticsStream
            .filter { it.dataType == "illumination" }

        // 2. 开灯事件流（亮度突增，推断有人）
        val lightingEvents = detectLightingEvents(illuminationStream)
        lightingEvents.print("Lighting Events")

        //  3.通过co2持续上升判断有人
        val co2IncreaseStream = sensorDataStream
            .filter { it.dataType == "co2" }
            .keyBy { it.sensorName }
            .window(SlidingEventTimeWindows.of(
                Duration.ofMinutes(5),
                Duration.ofMinutes(1)
            ))
            .process(
                object : ProcessWindowFunction<SensorData, Boolean, String, TimeWindow>() {
                    override fun process(
                        key: String,
                        context: ProcessWindowFunction<SensorData, Boolean, String, TimeWindow>.Context?,
                        elements: Iterable<SensorData>,
                        out: Collector<Boolean>
                    ) {
                        val values = elements.map { it.value }
                        val isIncreasing = values.zipWithNext().all { (a, b) -> a <= b }
                        out.collect(isIncreasing)
                    }
                }
            ).name("CO2 Increase Detection")


        // 4. 合并“开灯”和“CO2上升”事件流，进入状态管理
        val occupancyEventStream: DataStream<Boolean> = lightingEvents
            .union(co2IncreaseStream)
            .keyBy { "occupancy_state" }  // 使用一个固定的key来管理状态
            .process(object : KeyedProcessFunction<String, Boolean, Boolean>() {

                // 保存当前占用状态：true=有人, false=无人
                private val occStateDesc = ValueStateDescriptor<Boolean>("occState", Boolean::class.java)
                // 保存上次注册的定时器时间戳
                private val timerStateDesc = ValueStateDescriptor<Long>("idleTimer", Long::class.java)

                override fun processElement(
                    value: Boolean,
                    ctx: Context,
                    out: Collector<Boolean>
                ) {
                    val occState = runtimeContext.getState(occStateDesc)
                    val timerState = runtimeContext.getState(timerStateDesc)
                    val prev = occState.value() ?: false

                    // 如果当前收到一个“有人”事件，而之前状态是“无人”，则状态切换 -> “有人”
                    if (!prev && value) {
                        logger.info("Occupancy State: Unoccupied -> Occupied")
                        occState.update(true)
                        out.collect(true)  // 写入 Kafka：有人
                    }

                    // 无论之前状态是什么，只要收到“有人”事件，都要重置（或重注册）超时定时器
                    // 删除旧定时器（如果存在）
                    val prevTimerTs = timerState.value()
                    if (prevTimerTs != null) {
                        ctx.timerService().deleteProcessingTimeTimer(prevTimerTs)
                    }
                    // 注册一个新的定时器：当前处理时间 + 1 小时
                    val newTimerTs = ctx.timerService().currentProcessingTime() + OCCUPANCY_IDLE_TIMEOUT_MS
                    ctx.timerService().registerProcessingTimeTimer(newTimerTs)
                    timerState.update(newTimerTs)
                }

                override fun onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector<Boolean>) {
                    val occState = runtimeContext.getState(occStateDesc)
                    val prev = occState.value() ?: false
                    // 定时器触发时，如果当前状态还是“有人”，说明 1 小时内没有新“有人”事件，切换到“无人”
                    if (prev) {
                        logger.info("Occupancy State: Occupied -> Unoccupied (timeout at {})", timestamp)
                        occState.update(false)
                        out.collect(false)  // 写入 Kafka：无人
                    }
                    // 清空定时器状态
                    runtimeContext.getState(timerStateDesc).clear()
                }
            })
            .name("Occupancy State Controller")

        // 输出到Kafka
        occupancyEventStream.sinkTo(createOccupancyKafkaSink()).name("Kafka Occupancy Detection Sink")

        // 打印输出流
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