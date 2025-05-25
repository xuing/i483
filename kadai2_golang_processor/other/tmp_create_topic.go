package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	Partitions = 1 // 一般学生作业1分区即可
	Replicas   = 1 // 单节点Kafka请设为1
)

// 创建Topic
func createTopic(topic string, partitions, replicas int) error {
	conn, err := kafka.Dial("tcp", "150.65.230.59:9092")
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %v", err)
	}
	defer conn.Close()

	_, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicas,
	})
	return err
}

func main() {
	topics := []string{
		"i483-sensors-s2510082-co2_threshold-crossed",
		"i483-sensors-s2510082-BH1750_avg-illumination",
	}

	for _, t := range topics {
		fmt.Printf("Creating topic: %s ...\n", t)
		err := createTopic(t, Partitions, Replicas)
		if err != nil {
			log.Printf("Failed to create topic %s: %v", t, err)
		} else {
			fmt.Printf("Topic %s created or already exists.\n", t)
		}
	}
	fmt.Println("Done.")
}
