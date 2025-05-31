package org.jaist.flink.s2510082

import org.apache.flink.cep.CEP
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.datastream.DataStream
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

/**
 * 宿舍房间占用检测事件流处理器
 * 使用Flink CEP检测以下占用模式：
 * 1. 开灯事件：亮度突然升高（而不是缓慢爬升）
 * 2. CO2持续增长或维持在高位
 */
class OccupancyDetectionEventStream(private val sensorStatsStream: DataStream<SensorStats>) {

    companion object {
        private val logger = LoggerFactory.getLogger(OccupancyDetectionEventStream::class.java)

        // 检测阈值配置
        private const val LIGHT_SUDDEN_INCREASE_THRESHOLD = 100.0  // 亮度突然增加阈值
        private const val CO2_HIGH_THRESHOLD = 800.0              // CO2高浓度阈值 (ppm)
        private const val CO2_INCREASE_THRESHOLD = 100.0          // CO2增长阈值
        private const val CO2_SUSTAINED_DURATION_MINUTES = 5      // CO2维持高位的时间（分钟）
    }

    /**
     * 处理传感器统计流以检测占用事件
     * 应用CEP模式匹配来识别基于传感器数据的占用事件
     *
     * @return 满足占用检测条件的SensorStats数据流
     */
    fun detectOccupancyEvents(): DataStream<SensorStats> {
        logger.info("开始设置宿舍房间占用检测CEP模式")

        // 检测模式1：开灯事件（亮度突然升高）
        val lightingPattern = detectLightingEvents(sensorStatsStream.filter {
            it.dataType.equals("illumination", ignoreCase = true)
        })


    }

    /**
     * 检测开灯事件：亮度突然升高 illumination max - min > threshold
     */
    private fun detectLightingEvents(illuminationStream: DataStream<SensorStats>): DataStream<SensorStats> {

        // 定义亮度突然升高的CEP模式
        val lightPattern = Pattern.begin<SensorStats>("light_on")
            .where(SimpleCondition.of { value: SensorStats ->
                (value.maxValue - value.minValue) > LIGHT_SUDDEN_INCREASE_THRESHOLD
            })
            .within(Duration.ofMinutes(1))  // 在1分钟内检测到的亮度变化

        // 应用CEP模式并提取匹配结果
        return CEP.pattern(illuminationStream, lightPattern)
            .select { pattern ->
                val lightOnEvent = pattern["light_on"]?.first()
                logger.info("检测到开灯事件: {}", lightOnEvent)
                lightOnEvent!!
            }
    }

    /**
     * 检测CO2持续增长或维持高位事件
     */
    private fun detectCO2Events(keyedStream: DataStream<SensorStats>): DataStream<SensorStats> {
        logger.info("设置CO2持续增长/高位检测模式")

        // 模式1：CO2持续增长
        val co2IncreasePattern = Pattern.begin<SensorStats>("co2_start")
            .where(SimpleCondition.of { value: SensorStats ->
                value.dataType.contains("co2", ignoreCase = true) ||
                value.dataType.contains("carbon", ignoreCase = true)
            })
            .next("co2_increase")
            .where(SimpleCondition.of { value: SensorStats ->
                // 简化条件：检查CO2值是否超过增长阈值
                (value.dataType.contains("co2", ignoreCase = true) ||
                 value.dataType.contains("carbon", ignoreCase = true)) &&
                value.avgValue > (CO2_INCREASE_THRESHOLD + 400.0)  // 基础值 + 增长阈值
            })
            .within(Duration.ofMinutes(10))

        // 模式2：CO2维持高位
        val co2HighPattern = Pattern.begin<SensorStats>("co2_high")
            .where(SimpleCondition.of { value: SensorStats ->
                val isCO2 = value.dataType.contains("co2", ignoreCase = true) ||
                           value.dataType.contains("carbon", ignoreCase = true)
                val isHigh = value.avgValue > CO2_HIGH_THRESHOLD

                if (isCO2 && isHigh) {
                    logger.debug("检测到高CO2浓度: 传感器={}, 值={}", value.sensorName, value.avgValue)
                }

                isCO2 && isHigh
            })
            .timesOrMore(3)  // 至少连续3次检测到高值
            .within(Duration.ofMinutes(CO2_SUSTAINED_DURATION_MINUTES.toLong()))
        
        // 应用CO2增长模式
        val co2IncreaseStream = CEP.pattern(keyedStream, co2IncreasePattern)
            .select(object : PatternSelectFunction<SensorStats, SensorStats> {
                override fun select(pattern: MutableMap<String, MutableList<SensorStats>>): SensorStats {
                    val co2Event = pattern["co2_increase"]?.first()
                    logger.info("检测到CO2增长事件: {}", co2Event)
                    return co2Event!!
                }
            })
        
        // 应用CO2高位维持模式
        val co2HighStream = CEP.pattern(keyedStream, co2HighPattern)
            .select(object : PatternSelectFunction<SensorStats, SensorStats> {
                override fun select(pattern: MutableMap<String, MutableList<SensorStats>>): SensorStats {
                    val co2Events = pattern["co2_high"]
                    val latestEvent = co2Events?.last()
                    logger.info("检测到CO2高位维持事件: 连续{}次高值, 最新事件: {}", 
                        co2Events?.size, latestEvent)
                    return latestEvent!!
                }
            })
        
        // 合并两种CO2检测结果
        return co2IncreaseStream.union(co2HighStream)
    }
}