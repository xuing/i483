package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// List of topics to subscribe to
var topics = []string{
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

const StudentID = "s2510082"
const MqttBroker = "tcp://150.65.230.59:1883"
const baseTopic = "i483/sensors/" + StudentID + "/"

func main() {
	opts := mqtt.NewClientOptions().AddBroker(MqttBroker)
	opts.SetClientID("go-mqtt-subscriber")
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("✅ Connected to MQTT broker")
		for _, sub := range topics {
			full := baseTopic + sub
			token := c.Subscribe(full, 0, func(client mqtt.Client, msg mqtt.Message) {
				key := strings.TrimPrefix(msg.Topic(), baseTopic)
				log.Printf("📥 %s = %s\n", key, msg.Payload())
			})
			token.Wait()
			if token.Error() != nil {
				log.Printf("❌ Failed to subscribe: %s: %v\n", sub, token.Error())
			} else {
				log.Printf("📡 Listening to: %s\n", full)
			}
		}
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("⚠️ Connection lost: %v\n", err)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Block the main thread until interruption signal received
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("🛑 Exiting program")
	client.Disconnect(250)
}
