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

const studentID = "s2510082"
const broker = "tcp://150.65.230.59:1883"
const baseTopic = "i483/sensors/" + studentID + "/"

func main() {
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID("go-mqtt-subscriber")
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("✅ 已连接到 MQTT 服务器")
		for _, sub := range topics {
			full := baseTopic + sub
			token := c.Subscribe(full, 0, func(client mqtt.Client, msg mqtt.Message) {
				key := strings.TrimPrefix(msg.Topic(), baseTopic)
				fmt.Printf("📥 %s = %s\n", key, msg.Payload())
			})
			token.Wait()
			if token.Error() != nil {
				log.Printf("❌ 订阅失败: %s: %v\n", sub, token.Error())
			} else {
				log.Printf("📡 正在监听: %s\n", full)
			}
		}
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("⚠️ 连接丢失: %v\n", err)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// 阻止程序退出
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("🛑 退出程序")
	client.Disconnect(250)
}
