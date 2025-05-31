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
	KafkaBroker  = "150.65.230.59:9092"
	studentID    = "s2510082"
	Co2Threshold = 700 // CO2 threshold in ppm
)

// SensorData Data structure definition
type SensorData struct {
	Value     float64
	Timestamp time.Time
}

// Global data storage
var (
	illuminationData []SensorData
	dataLock         sync.RWMutex
	lastCo2Status    = "unknown" // Track last CO2 status for threshold detection
	statusLock       sync.RWMutex
)

func makeSensorsTopic(sensor string) string {
	return "i483-sensors-" + studentID + "-" + sensorToKafka(sensor)
}

func sensorToKafka(s string) string {
	return strings.ReplaceAll(s, "/", "-")
}

// Kafka write function
func writeToKafka(topic string, message string) error {
	w := kafka.Writer{
		Addr:                   kafka.TCP(KafkaBroker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
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

// Parse sensor data value
func parseValue(message string) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(message), 64)
	return value, err
}

// BH1750 illumination data collector
func illuminationDataCollector() {
	topic := makeSensorsTopic("BH1750/illumination")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{KafkaBroker},
		Topic:       topic,
		GroupID:     topic + "-golang_collector",
		StartOffset: kafka.LastOffset,
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

		// Parse data
		value, err := parseValue(string(m.Value))
		if err != nil {
			log.Printf("❗ [Illumination Collector] Failed to parse value '%s': %v", string(m.Value), err)
			continue
		}

		// Store data
		dataLock.Lock()
		illuminationData = append(illuminationData, SensorData{
			Value:     value,
			Timestamp: time.Now(),
		})

		// Clean up data older than 5 minutes
		cutoff := time.Now().Add(-5 * time.Minute)
		var filtered []SensorData
		for _, data := range illuminationData {
			if data.Timestamp.After(cutoff) {
				filtered = append(filtered, data)
			}
		}
		illuminationData = filtered
		dataLock.Unlock()

		fmt.Printf("✅ [Illumination] Received: %.2f (stored %d values, timestamp: %s)\n", value, len(illuminationData), m.Time)
	}
}

// CO2 data collector
func co2DataCollector() {
	topic := makeSensorsTopic("SCD41/co2")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{KafkaBroker},
		Topic:       topic,
		GroupID:     topic + "-golang_collector",
		StartOffset: kafka.LastOffset,
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

		// Parse CO2 data
		value, err := parseValue(string(m.Value))
		if err != nil {
			log.Printf("❗ [CO2 Collector] Failed to parse value '%s': %v", string(m.Value), err)
			continue
		}

		fmt.Printf("✅ [CO2] Received: %.2f ppm (timestamp: %s)\n", value, m.Time)

		// CO2 threshold detection
		checkCO2Threshold(value)
	}
}

// Rolling Average: Calculate the average of the past 5 minutes every 30 seconds
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

		// Calculate average
		var sum float64
		for _, data := range illuminationData {
			sum += data.Value
		}
		average := sum / float64(dataCount)
		dataLock.RUnlock()

		// Publish average to Kafka
		avgMessage := fmt.Sprintf("%.2f", average)
		err := writeToKafka(avgTopic, avgMessage)
		if err != nil {
			log.Printf("❗ [Rolling Average] Failed to publish: %v", err)
		} else {
			fmt.Printf("📊 [Rolling Average] Published: %.2f (from %d samples)\n", average, dataCount)
		}
	}
}

// Threshold Detection: CO2 threshold detection
func checkCO2Threshold(co2Value float64) {
	statusLock.Lock()
	defer statusLock.Unlock()

	var currentStatus string
	var message string

	if co2Value > Co2Threshold {
		currentStatus = "above"
		message = "yes"
	} else {
		currentStatus = "below"
		message = "no"
	}
	// Only send a message when status changes
	if lastCo2Status != currentStatus {
		actuatorTopic := "i483-actuators-" + studentID + "-co2_threshold_crossed"

		err := writeToKafka(actuatorTopic, message)
		if err != nil {
			log.Printf("❗ [CO2 Threshold Actuator] Failed to publish: %v", err)
		} else {
			fmt.Printf("🚨 [CO2 Threshold Actuator] Status changed: %.2f ppm -> %s (sent: %s to %s)\n",
				co2Value, currentStatus, message, actuatorTopic)
		}

		lastCo2Status = currentStatus
	}
}

func main() {
	fmt.Println("🚀 Advanced Kafka Sensor Processor starting...")
	fmt.Printf("📋 Student ID: %s\n", studentID)
	fmt.Println("🎯 Features:")
	fmt.Println("   - Rolling Average: BH1750 illumination (every 30s, 5min window) -> published to i483-sensors-s2510082-BH1750_avg-illumination")
	fmt.Println("   - Threshold Detection: CO2 > 700ppm alert -> published to i483-actuators-s2510082-co2_threshold_crossed")
	fmt.Println()

	// Initialize status
	statusLock.Lock()
	lastCo2Status = "unknown"
	statusLock.Unlock()

	// Start dedicated data collectors
	go illuminationDataCollector()
	go co2DataCollector()

	// Start rolling average processor
	go rollingAverageProcessor()

	fmt.Println("✅ All processors started successfully!")
	select {} // Block forever
}
