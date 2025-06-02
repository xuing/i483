package org.jaist.flink.s2510082

/**
 * Sensor key for grouping - POJO compatible
 */
data class SensorKey(
    var studentId: String = "",
    var sensorName: String = "",
    var dataType: String = ""
)

/**
 * Sensor data class - POJO compatible
 */
data class SensorData(
    var sensorName: String = "",
    var dataType: String = "",
    var topic: String = "",
    var timestamp: Long = 0L,
    var value: Double = 0.0,
    var studentId: String = ""
)

/**
 * Aggregated statistics result class - POJO compatible
 */
data class SensorStats(
    var sensorName: String = "",
    var dataType: String = "",
    var studentId: String = "",
    var windowStart: Long = 0L,
    var windowEnd: Long = 0L,
    var minValue: Double = 0.0,
    var maxValue: Double = 0.0,
    var avgValue: Double = 0.0,
    var count: Long = 0L
)

data class AnalyticsKafkaRecord(
    var topic: String = "",
    var value: String = ""
)

/**
 * Aggregation accumulator
 */
data class StatsAccumulator(
    var sum: Double = 0.0,
    var count: Long = 0,
    var min: Double = Double.MAX_VALUE,
    var max: Double = Double.MIN_VALUE
)