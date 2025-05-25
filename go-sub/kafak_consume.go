package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	Kafka_broker = "150.65.230.59:9092"
)

var sensors = []string{
	"SCD41/co2",
	"SCD41/temperature",
	"SCD41/humidity",
	"BH1750/illumination",
	"RPR0521RS/ambient_light",
	"RPR0521RS/proximity",
	"RPR0521RS/illumination",
	"RPR0521RS/infrared_illumination",
	"DPS310/temperature",
	"DPS310/air_pressure",
	"DPS310/altitude",
}

const studentID = "s2510082"

func makeTopic(sensor string) string {
	// 转换 i483/sensors/s2510082/SENSOR/TYPE → i483-sensors-s2510082-SENSOR-TYPE
	return "i483-sensors-" + studentID + "-" + sensorToKafka(sensor)
}

func sensorToKafka(s string) string {
	return strings.ReplaceAll(s, "/", "-")
}

func startReader(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{Kafka_broker},
		Topic:       topic,
		GroupID:     topic, // 每个 reader 单独的 group，避免 rebalance
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})

	fmt.Printf("📡 正在监听 Kafka topic: %s\n", topic)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		m, err := r.ReadMessage(ctx)
		cancel()
		if err != nil {
			log.Printf("⚠️ [%s] 读取失败: %v", topic, err)
			continue
		}
		fmt.Printf("✅ [%s] offset %d: %s\n", topic, m.Offset, string(m.Value))
	}
}

func main() {
	fmt.Println("🚀 启动多传感器 Kafka 监听器")

	for _, s := range sensors {
		topic := makeTopic(s)
		go startReader(topic)
	}

	select {} // 阻止主线程退出
}
