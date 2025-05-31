"""I483 - Kadai1 - XU Pengfei(2510082)
Multi-sensor Data Acquisition System - Async Implementation
"""
import asyncio

from machine import I2C, Pin
from umqtt.robust import MQTTClient

from sensors.bh1750 import BH1750
from sensors.dps310 import DPS310
from sensors.rpr0521rs import RPR0521RS
from sensors.scd41 import SCD41
from sensors.sensor import Sensor
from utils.utils import show_mac_address, connect_wifi, sync_rtc, current_time


class AsyncSensorManager:
    """Async Sensor Manager - Unified management of all sensors"""

    def __init__(self):
        # Initialize I2C bus
        self.i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=100000)

        self.scd41: Sensor = SCD41(self.i2c)
        self.bh1750: Sensor = BH1750(self.i2c)
        self.rpr0521rs: Sensor = RPR0521RS(self.i2c)
        self.dps310: Sensor = DPS310(self.i2c)
        self.sensors = [self.scd41, self.bh1750, self.rpr0521rs, self.dps310]

        # MQTT client
        self.mqtt_client = setup_mqtt()

        # Initialize all sensors
        self._init_sensors()

    def _init_sensors(self):
        """Initialize all sensors"""
        print("\n=== Initializing Sensors ===")
        for sensor in self.sensors:
            sensor.start()
            print(f"[INFO] {sensor.name} initialized successfully")
        print("\n=== All Sensors Initialized ===")

    def read_all_sensors(self):
        """Read data from all sensors"""
        for sensor in self.sensors:
            try:
                # Read data from each sensor
                sensor.read()
            except Exception as e:
                print(f"[ERROR] Failed to read {sensor.name}: {e}")

    def display_sensor_data(self):
        """Display all sensor data"""
        print(f"\n=== Sensor Data [{current_time()}] ===")

        for sensor in self.sensors:
            display_str = sensor.__class__.display(sensor.data)
            print(display_str)

    def mqtt_publish(self, student_id="s2510082"):
        """Publish sensor data to MQTT broker"""
        if not self.mqtt_client:
            return

        topic_base = f"i483/sensors/{student_id}/"

        for sensor in self.sensors:
            for key, value in sensor.data.items():
                topic_suffix = f"{sensor.name}/{key}"
                if isinstance(value, (int, float, str)):
                    # Only publish valid data types
                    full_topic = topic_base + topic_suffix
                    formatted_value = f"{value:.2f}" if isinstance(value, float) else str(value)
                    print(f"[MQTT] Publishing {full_topic}: {formatted_value}")
                    self.mqtt_client.publish(full_topic, formatted_value)

    def set_mqtt_client(self, client):
        """Set MQTT client"""
        self.mqtt_client = client

    async def sensor_reader_async(self):
        """Async sensor reading task"""
        task_id = id(asyncio.current_task())  # 获取当前任务的唯一ID
        print(f"[DEBUG] sensor_reader_async started, task_id: {task_id}")

        while True:
            print(f"[DEBUG] Task {task_id} - Starting sensor read cycle")
            self.read_all_sensors()
            await asyncio.sleep(1)
            self.display_sensor_data()
            self.mqtt_publish(student_id="s2510082")
            print(f"[DEBUG] Task {task_id} - Cycle complete, sleeping for 15s")
            await asyncio.sleep(15)

    async def mqtt_poller(self):
        """Async MQTT message polling task"""
        while True:
            self.mqtt_client.check_msg()
            await asyncio.sleep(1)


# Global variables for LED control
led = Pin(2, Pin.OUT)
led_should_blink = False


def mqtt_callback(topic, msg):
    """MQTT message callback"""
    global led_should_blink
    topic = topic.decode() if isinstance(topic, bytes) else topic
    msg = msg.decode() if isinstance(msg, bytes) else msg

    print(f"[MQTT] Received topic: {topic}, msg: {msg}")
    if topic.endswith("/co2_threshold_crossed"):
        led_should_blink = (msg == "yes")
        if not led_should_blink:
            led.value(0)


def setup_mqtt() -> MQTTClient:
    """Setup MQTT connection"""
    client = MQTTClient(client_id="s2510082", server="150.65.230.59")
    res = client.connect()
    print(f"MQTT connection result: {res}")
    client.set_callback(mqtt_callback)
    res = client.subscribe("i483/actuators/s2510082/co2_threshold_crossed")
    print(f"MQTT subscribe result: {res}")
    return client


async def led_controller():
    """Async LED controller task"""
    global led_should_blink
    while True:
        if led_should_blink:
            led.value(1)
            await asyncio.sleep(0.25)
            led.value(0)
            await asyncio.sleep(0.25)
        else:
            led.value(0)
            await asyncio.sleep(0.2)


def main():
    """Main function"""
    print("\n=== I483 - Kadai1 - XU Pengfei(2510082) ===")
    print("Multi-sensor Data Acquisition System - Async Implementation")

    show_mac_address()

    if connect_wifi():
        sync_rtc()
    else:
        print("WiFi connection failed, unable to synchronize RTC time")

    # Initialize sensor manager and MQTT client
    sensor_manager = AsyncSensorManager()

    print("\nStarting async sensor data acquisition...")
    print("Press Ctrl+C to stop the program")

    try:
        # sensor_manager.sensor_reader()
        asyncio.run(asyncio.gather(
            sensor_manager.sensor_reader_async(),
            sensor_manager.mqtt_poller(),
            led_controller()
        ))
    except Exception as e:
        print("sensor_reader error:", e)
    except KeyboardInterrupt:
        print("\nProgram stopped")
    finally:
        print("Cleaning up resources...")
        led.value(0)


if __name__ == "__main__":
    main()
