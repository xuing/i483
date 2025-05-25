package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	KafkaBroker = "150.65.230.59:9092"
	studentID   = "s2510082"
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

// 数据结构定义
type SensorData struct {
	Value     float64
	Timestamp time.Time
}

// 全局数据存储
var (
	illuminationData []SensorData
	dataLock         sync.RWMutex
	lastCo2Status    string // "above" or "below" 记录上次CO2状态
	statusLock       sync.RWMutex
)

func makeTopic(sensor string) string {
	return "i483-sensors-" + studentID + "-" + sensorToKafka(sensor)
}

func sensorToKafka(s string) string {
	return strings.ReplaceAll(s, "/", "-")
}

// Kafka写入函数
func writeToKafka(topic string, message string) error {
	w := kafka.Writer{
		Addr:  kafka.TCP(KafkaBroker),
		Topic: topic,
	}
	defer func(w *kafka.Writer) {
		err := w.Close()
		if err != nil {
			log.Println(err)
		}
	}(&w)

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(message),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to write message: %v", err)
	}
	return nil
}

// 解析传感器数据值
func parseValue(message string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(message), 64)
	return value, err
}

// BH1750 illumination数据收集器
func illuminationDataCollector() {
	topic := makeTopic("BH1750/illumination")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{KafkaBroker},
		Topic:       topic,
		GroupID:     topic + "-illumination-collector",
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer func(r *kafka.Reader) {
		err := r.Close()
		if err != nil {
			log.Fatalf("failed to close reader: %v", err)
		}
	}(r)

	fmt.Printf("📡 [Illumination Collector] Listening to: %s\n", topic)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		m, err := r.ReadMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("❗ [Illumination Collector] Error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// 解析数据
		value, err := parseValue(string(m.Value))
		if err != nil {
			log.Printf("❗ [Illumination Collector] Failed to parse value '%s': %v", string(m.Value), err)
			continue
		}

		// 存储数据
		dataLock.Lock()
		illuminationData = append(illuminationData, SensorData{
			Value:     value,
			Timestamp: time.Now(),
		})

		// 清理5分钟以前的数据
		cutoff := time.Now().Add(-5 * time.Minute)
		var filtered []SensorData
		for _, data := range illuminationData {
			if data.Timestamp.After(cutoff) {
				filtered = append(filtered, data)
			}
		}
		illuminationData = filtered
		dataLock.Unlock()

		fmt.Printf("✅ [Illumination] Received: %.2f (stored %d values)\n", value, len(illuminationData))
	}
}

// CO2数据收集器
func co2DataCollector() {
	topic := makeTopic("SCD41/co2")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{KafkaBroker},
		Topic:       topic,
		GroupID:     topic + "-co2-collector",
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer func(r *kafka.Reader) {
		err := r.Close()
		if err != nil {
			log.Fatalf("failed to close reader: %v", err)
		}
	}(r)

	fmt.Printf("📡 [CO2 Collector] Listening to: %s\n", topic)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		m, err := r.ReadMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("❗ [CO2 Collector] Error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// 解析CO2数据
		value, err := parseValue(string(m.Value))
		if err != nil {
			log.Printf("❗ [CO2 Collector] Failed to parse value '%s': %v", string(m.Value), err)
			continue
		}

		fmt.Printf("✅ [CO2] Received: %.2f ppm\n", value)

		// CO2阈值检测
		checkCO2Threshold(value)
	}
}

// Rolling Average: 每30秒计算过去5分钟的平均值
func rollingAverageProcessor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	avgTopic := "i483-sensors-" + studentID + "-BH1750_avg-illumination"
	fmt.Printf("🔄 [Rolling Average] Will publish to: %s\n", avgTopic)

	for range ticker.C {
		dataLock.RLock()
		dataCount := len(illuminationData)

		if dataCount == 0 {
			dataLock.RUnlock()
			fmt.Printf("⏳ [Rolling Average] No illumination data available\n")
			continue
		}

		// 计算平均值
		var sum float64
		for _, data := range illuminationData {
			sum += data.Value
		}
		average := sum / float64(dataCount)
		dataLock.RUnlock()

		// 发布平均值到Kafka
		avgMessage := fmt.Sprintf("%.2f", average)
		err := writeToKafka(avgTopic, avgMessage)
		if err != nil {
			log.Printf("❗ [Rolling Average] Failed to publish: %v", err)
		} else {
			fmt.Printf("📊 [Rolling Average] Published: %.2f (from %d samples)\n", average, dataCount)
		}
	}
}

// Threshold Detection: CO2阈值检测
func checkCO2Threshold(co2Value float64) {
	statusLock.Lock()
	defer statusLock.Unlock()

	var currentStatus string
	var message string

	if co2Value > 700 {
		currentStatus = "above"
		message = "yes"
	} else {
		currentStatus = "below"
		message = "no"
	}
	// 只有状态改变时才发送消息
	if lastCo2Status != currentStatus {
		thresholdTopic := "i483-sensors-" + studentID + "-co2_threshold-crossed"

		err := writeToKafka(thresholdTopic, message)
		if err != nil {
			log.Printf("❗ [CO2 Threshold] Failed to publish: %v", err)
		} else {
			fmt.Printf("🚨 [CO2 Threshold] Status changed: %.2f ppm -> %s (sent: %s)\n",
				co2Value, currentStatus, message)
		}

		lastCo2Status = currentStatus
	}
}

// 通用传感器数据读取器（用于显示其他传感器数据）
func startReader(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{KafkaBroker},
		Topic:       topic,
		GroupID:     topic + "-general",
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer func(r *kafka.Reader) {
		err := r.Close()
		if err != nil {
			log.Printf("<UNK> [Start Reader] Error closing reader: %v", err)
		}
	}(r)

	fmt.Printf("📡 [General] Listening to: %s\n", topic)

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		m, err := r.ReadMessage(ctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			log.Printf("❗ [%s] Error: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		fmt.Printf("✅ [%s] offset %d: %s\n", topic, m.Offset, string(m.Value))
	}
}

func main() {
	fmt.Println("🚀 Advanced Kafka Sensor Processor starting...")
	fmt.Printf("📋 Student ID: %s\n", studentID)
	fmt.Println("🎯 Features:")
	fmt.Println("   - Rolling Average: BH1750 illumination (every 30s, 5min window)")
	fmt.Println("   - Threshold Detection: CO2 > 700ppm alert")
	fmt.Println()

	// 初始化状态
	statusLock.Lock()
	lastCo2Status = "unknown"
	statusLock.Unlock()

	// 启动专门的数据收集器
	go illuminationDataCollector()
	go co2DataCollector()

	// 启动rolling average处理器
	go rollingAverageProcessor()

	//// 启动其他传感器的通用读取器
	//for _, s := range sensors {
	//	// 跳过已经有专门处理器的传感器
	//	if s == "BH1750/illumination" || s == "SCD41/co2" {
	//		continue
	//	}
	//
	//	topic := makeTopic(s)
	//	go startReader(topic)
	//}

	fmt.Println("✅ All processors started successfully!")
	select {} // 永远阻塞
}
