package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const topic = "i483/sensors/reference/#"
const clientID = "go-ref-subscriber"

func main() {
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(clientID)

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Printf("✅ 已连接 MQTT broker：%s\n", broker)
		if token := c.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			fmt.Printf("📥 %s => %s\n", msg.Topic(), msg.Payload())
		}); token.Wait() && token.Error() != nil {
			log.Fatalf("❌ 订阅失败: %v", token.Error())
		} else {
			fmt.Printf("📡 正在监听 topic：%s\n", topic)
		}
	}

	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.Printf("⚠️ 连接丢失: %v\n", err)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("❌ 连接失败: %v", token.Error())
	}

	// 等待退出信号
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	<-sigs

	fmt.Println("🛑 程序终止，断开连接...")
	client.Disconnect(250)
}
