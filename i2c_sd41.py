from machine import I2C, Pin
import time
import struct
# 我是ESP32的板子配合SD41，我希望你阅读这个C语言的SD41库，为我写一个mircoPython的库，参考这个库的做法，保持最佳实践。
# SCD41 I2C アドレス（固定）
SCD41_ADDR = 0x62

# I2C 初期化 (GPIO番号は環境に合わせて)
i2c = I2C(0, scl=Pin(19), sda=Pin(18), freq=100000)

# CRC 計算（Sensirion方式）
def calc_crc(data):
    crc = 0xFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = (crc << 1) ^ 0x31
            else:
                crc <<= 1
            crc &= 0xFF
    return crc

# コマンド送信（引数：16ビット整数）
def send_command(cmd):
    i2c.writeto(SCD41_ADDR, cmd.to_bytes(2, 'big'))

# 計測データ読み取り
def read_measurement():
    send_command(0xEC05)  # read_measurement コマンド
    time.sleep_ms(2)
    raw = i2c.readfrom(SCD41_ADDR, 9)

    # データ + CRC を抽出
    co2_raw = raw[0:2]
    temp_raw = raw[3:5]
    humid_raw = raw[6:8]

    if calc_crc(raw[0:2]) != raw[2] or calc_crc(raw[3:5]) != raw[5] or calc_crc(raw[6:8]) != raw[8]:
        print("CRC error")
        return None

    co2 = int.from_bytes(co2_raw, 'big')
    temp = -45 + 175 * int.from_bytes(temp_raw, 'big') / 65535
    humid = 100 * int.from_bytes(humid_raw, 'big') / 65535

    return co2, temp, humid

# 計測開始
def stop_measurement():
    send_command(0x3f86)


try:
    # Stop any running measurement (防止设备忙)
    stop_measurement()
except:
    pass  # 忽略错误

# 然后再 start_measurement

print("Starting periodic measurement...")
send_command(0x21B1)  # start_periodic_measurement
time.sleep(5)  # 初回測定まで5秒必要

# ループで定期取得
while True:
    result = read_measurement()
    if result:
        co2, temp, humid = result
        print("CO2: {} ppm, Temp: {:.2f} C, Humidity: {:.2f}%".format(co2, temp, humid))
    time.sleep(5)
