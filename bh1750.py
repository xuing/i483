from machine import I2C, Pin
import time

# 定义各个工作模式的常量
CONT_H_RES_MODE = 0x10  # Continuous high resolution mode (1 lx)
CONT_H_RES_MODE2 = 0x11  # Continuous high resolution mode 2 (0.5 lx)
CONT_L_RES_MODE = 0x13  # Continuous low resolution mode (4 lx)
ONE_TIME_H_RES_MODE = 0x20  # One-time high resolution mode (1 lx)
ONE_TIME_H_RES_MODE2 = 0x21  # One-time high resolution mode 2 (0.5 lx)
ONE_TIME_L_RES_MODE = 0x23  # One-time low resolution mode (4 lx)


class BH1750:
    """基于 MicroPython I2C 的 BH1750 环境光传感器驱动。
    参考文档：bh1750fvi-e-186247.pdf
    """

    PWR_DOWN = 0x00
    PWR_ON = 0x01
    RESET = 0x07

    def __init__(self, i2c: I2C, address=0x23, mode=CONT_H_RES_MODE):
        self.i2c = i2c
        self.address = address
        self._mode = mode
        self._measurement_accuracy = 1.0  # 默认因子为1.0, 若需要可根据实际校准调整

    def power(self, on: bool = True):
        """使能或关闭传感器电源"""
        cmd = self.PWR_ON if on else self.PWR_DOWN
        self._send_cmd(cmd)
        # 根据 datasheet，Power On 后建议等待至少10ms
        time.sleep_ms(10)

    def reset(self):
        """重置传感器，只有在上电状态下有效"""
        self._send_cmd(self.RESET)
        # datasheet建议重置后等待10ms以上
        time.sleep_ms(10)

    def set_mode(self, mode=None):
        """设置传感器测量模式，可选模式由常量定义"""
        if mode is not None:
            self._mode = mode
        self._send_cmd(self._mode)
        # 对于连续高分辨率模式，第一次采样后建议等待约150ms
        if self._mode in (CONT_H_RES_MODE, CONT_H_RES_MODE2):
            time.sleep_ms(150)
        elif self._mode in (CONT_L_RES_MODE,):
            time.sleep_ms(20)
        # 如果使用一次性模式，则调用后需重新触发测量

    def _send_cmd(self, cmd: int):
        try:
            self.i2c.writeto(self.address, bytes([cmd]))
        except Exception as e:
            print("Error sending command:", e)

    def read_light(self) -> float:
        """读取传感器返回数据，计算光照强度（单位：lux）
        datasheet中给出的换算公式：lux = (测量值) / 1.2 / measurement_accuracy
        """
        try:
            raw = self.i2c.readfrom(self.address, 2)
            raw_val = (raw[0] << 8) | raw[1]
            lux = raw_val / 1.2 / self._measurement_accuracy
            return lux
        except Exception as e:
            print("Error reading light level:", e)
            return -1.0

    @property
    def measurement_accuracy(self):
        return self._measurement_accuracy

    @measurement_accuracy.setter
    def measurement_accuracy(self, val: float):
        if not 0.96 <= val <= 1.44:
            raise ValueError("Accuracy must be between 0.96 and 1.44")
        self._measurement_accuracy = val

    def power_down(self):
        """关闭传感器以降低功耗"""
        self.power(False)


def main():
    # 初始化 I2C (根据实际硬件设置正确的 scl 和 sda 引脚)
    i2c = I2C(0, scl=Pin(22), sda=Pin(21))

    # 创建 BH1750 实例，选择连续高分辨率模式
    sensor = BH1750(i2c, mode=CONT_H_RES_MODE)

    # 打开传感器电源并初始化
    sensor.power(True)
    sensor.reset()
    sensor.set_mode()  # 此处等待已内置

    try:
        while True:
            lux = sensor.read_light()
            if lux >= 0:
                print(f"Illuminance: {lux:.2f} lx")
            else:
                print("Failed to read sensor data.")
            # 轮询间隔可根据实际需求设置，确保大于传感器转换时间
            time.sleep(1)
    except KeyboardInterrupt:
        # 程序退出时关闭传感器
        sensor.power_down()
        print("Sensor powered down.")


if __name__ == "__main__":
    main()
