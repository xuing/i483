from machine import I2C, Pin
import time

class BH1750:
    """Driver for the BH1750 light sensor using MicroPython I2C."""

    PWR_DOWN = 0x00
    PWR_ON = 0x01
    RESET = 0x07

    # default address is 0x23 or 0x5C depending on ADDR pin
    def __init__(self, i2c: I2C, address=0x23):
        self.i2c = i2c
        self.address = address
        self._mode = 0x10  # default: Continuously H-Resolution Mode
        self._measurement_accuracy = 1.0

    def power(self, on: bool = True):
        self._send_cmd(self.PWR_ON if on else self.PWR_DOWN)

    def reset(self):
        self._send_cmd(self.RESET)

    def set_mode(self, mode=0x10):
        self._mode = mode
        self._send_cmd(self._mode)

    def _send_cmd(self, cmd: int):
        self.i2c.writeto(self.address, bytes([cmd]))

    def read_light(self) -> float:
        """Reads light level in lux."""
        raw = self.i2c.readfrom(self.address, 2)
        lux = (raw[0] << 8 | raw[1]) / 1.2 / self._measurement_accuracy
        return lux

    @property
    def measurement_accuracy(self):
        return self._measurement_accuracy

    @measurement_accuracy.setter
    def measurement_accuracy(self, val: float):
        if not 0.96 <= val <= 1.44:
            raise ValueError("Accuracy must be between 0.96 and 1.44")
        self._measurement_accuracy = val


def main():
    # Setup I2C for ESP32
    i2c = I2C(0, scl=Pin(22), sda=Pin(21))
    sensor = BH1750(i2c)

    sensor.power(True)
    time.sleep_ms(10)
    sensor.reset()
    time.sleep_ms(10)
    sensor.set_mode()

    while True:
        lux = sensor.read_light()
        print(f"Illuminance: {lux:.2f} lx")
        time.sleep(1)


if __name__ == "__main__":
    main()
