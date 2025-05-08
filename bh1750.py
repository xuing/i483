from machine import I2C, Pin
import time

# Define constants for each operating mode
CONT_H_RES_MODE = 0x10  # Continuous high resolution mode (1 lx)
CONT_H_RES_MODE2 = 0x11  # Continuous high resolution mode 2 (0.5 lx)
CONT_L_RES_MODE = 0x13  # Continuous low resolution mode (4 lx)
ONE_TIME_H_RES_MODE = 0x20  # One-time high resolution mode (1 lx)
ONE_TIME_H_RES_MODE2 = 0x21  # One-time high resolution mode 2 (0.5 lx)
ONE_TIME_L_RES_MODE = 0x23  # One-time low resolution mode (4 lx)


class BH1750:
    """BH1750 ambient light sensor driver based on MicroPython I2C.
    Reference document: bh1750fvi-e-186247.pdf
    """

    PWR_DOWN = 0x00
    PWR_ON = 0x01
    RESET = 0x07

    def __init__(self, i2c: I2C, address=0x23, mode=CONT_H_RES_MODE):
        self.i2c = i2c
        self.address = address
        self._mode = mode
        self._measurement_accuracy = 1.0  # Default factor is 1.0, can be adjusted according to actual calibration if needed

    def power(self, on: bool = True):
        """Enable or disable sensor power"""
        cmd = self.PWR_ON if on else self.PWR_DOWN
        self._send_cmd(cmd)
        # According to the datasheet, it's recommended to wait at least 10ms after Power On
        time.sleep_ms(10)

    def reset(self):
        """Reset the sensor, only effective when powered on"""
        self._send_cmd(self.RESET)
        # Datasheet recommends waiting more than 10ms after reset
        time.sleep_ms(10)

    def set_mode(self, mode=None):
        """Set sensor measurement mode, available modes are defined by constants"""
        if mode is not None:
            self._mode = mode
        self._send_cmd(self._mode)
        # For continuous high resolution modes, it's recommended to wait about 150ms after the first sampling
        if self._mode in (CONT_H_RES_MODE, CONT_H_RES_MODE2):
            time.sleep_ms(150)
        elif self._mode in (CONT_L_RES_MODE,):
            time.sleep_ms(20)
        # If using one-time mode, measurement needs to be triggered again after calling

    def _send_cmd(self, cmd: int):
        try:
            self.i2c.writeto(self.address, bytes([cmd]))
        except Exception as e:
            print("Error sending command:", e)

    def read_light(self) -> float:
        """Read sensor data and calculate light intensity (unit: lux)
        Conversion formula from datasheet: lux = (measured value) / 1.2 / measurement_accuracy
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
        """Turn off the sensor to reduce power consumption"""
        self.power(False)


def main():
    # Initialize I2C (set the correct scl and sda pins according to actual hardware)
    i2c = I2C(0, scl=Pin(22), sda=Pin(21))

    # Create BH1750 instance, select continuous high resolution mode
    sensor = BH1750(i2c, mode=CONT_H_RES_MODE)

    # Turn on sensor power and initialize
    sensor.power(True)
    sensor.reset()
    sensor.set_mode()  # Wait time is already built-in here

    try:
        while True:
            lux = sensor.read_light()
            if lux >= 0:
                print(f"Illuminance: {lux:.2f} lx")
            else:
                print("Failed to read sensor data.")
            # Polling interval can be set according to actual needs, ensure it's greater than sensor conversion time
            time.sleep(1)
    except KeyboardInterrupt:
        # Turn off the sensor when exiting the program
        sensor.power_down()
        print("Sensor powered down.")


if __name__ == "__main__":
    main()
