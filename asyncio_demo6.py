import asyncio
import random
import collections
from umqtt.robust import MQTTClient
import network
from machine import Pin

from utils import connect_wifi

work_queue: collections.deque[int] = collections.deque((), 16)
work_to_do: asyncio.Event = asyncio.Event()
work_to_do.clear()

led = Pin(2, Pin.OUT)
led_state = False


def mqtt_callback(topic: str, msg: bytes):
    print(f"mqtt callback! {topic=}, msg: {msg.decode('UTF-8')}, binary data: {msg.hex()}")
    global led_state

    led_state = not led_state
    led.value(led_state)


def net_setup() -> MQTTClient:
    connect_wifi()

    mqtt_client: MQTTClient = MQTTClient(client_id="test", server="150.65.230.59")
    res = mqtt_client.connect()
    print(f"connect result: {res}")
    mqtt_client.set_callback(mqtt_callback)
    res = mqtt_client.subscribe("i483/mp/callme")
    print(f"subscribe result: {res}")
    return mqtt_client


client: MQTTClient = net_setup()


async def producer(name: str = "1"):
    while True:
        print(f"producer {name}: queing work!")
        rn = random.randint(100, 1000)
        work_queue.append(rn)
        work_to_do.set()
        await asyncio.sleep(rn / 1000)


def post(data: int):
    client.publish("i483/mp/test", f"{data}".encode(), qos=1)


async def consumer():
    while True:
        await work_to_do.wait()
        work: int = work_queue.popleft()
        print(f"consumer: consuming {work=}, items in queue: {len(work_queue)}")
        #stop ourselves from blocking in get
        if len(work_queue) == 0:
            work_to_do.clear()
        post(work)


async def poll_mqtt():
    while True:
        #client.wait_msg()
        client.check_msg()
        await asyncio.sleep(0.1)
        print(f"polling!")


async def all_tasks():
    _ = await asyncio.gather(
        producer("A"),
        producer("B"),
        producer("C"),
        consumer(),
        poll_mqtt())


asyncio.run(all_tasks())
