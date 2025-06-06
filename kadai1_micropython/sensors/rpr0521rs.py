# SPDX-FileCopyrightText: Copyright (c) 2025 XU pengfei
# SPDX-License-Identifier: MIT
import time

from micropython import const

from utils.i2c_helpers import RegisterStruct
from sensors.sensor import Sensor

# RPR-0521RS Device Address
RPR0521RS_DEVICE_ADDRESS = const(0x38)  # 7bit Address

# Register Addresses
RPR0521RS_SYSTEM_CONTROL = const(0x40)
RPR0521RS_MODE_CONTROL = const(0x41)
RPR0521RS_ALS_PS_CONTROL = const(0x42)
RPR0521RS_PS_CONTROL = const(0x43)
RPR0521RS_PS_DATA_LSB = const(0x44)
RPR0521RS_ALS_DATA0_LSB = const(0x46)
RPR0521RS_MANUFACT_ID = const(0x92)

# Part ID and Manufacturer ID
RPR0521RS_PART_ID_VAL = const(0x0A)
RPR0521RS_MANUFACT_ID_VAL = const(0xE0)

# Mode Control Register Values
RPR0521RS_MODE_CONTROL_MEASTIME_100_100MS = const(6 << 0)
RPR0521RS_MODE_CONTROL_PS_EN = const(1 << 6)
RPR0521RS_MODE_CONTROL_ALS_EN = const(1 << 7)

# ALS/PS Control Register Values
RPR0521RS_ALS_PS_CONTROL_LED_CURRENT_100MA = const(2 << 0)
RPR0521RS_ALS_PS_CONTROL_DATA1_GAIN_X1 = const(0 << 2)
RPR0521RS_ALS_PS_CONTROL_DATA0_GAIN_X1 = const(0 << 4)

# PS Control Register Values
RPR0521RS_PS_CONTROL_PS_GAINX1 = const(0 << 4)

# Default Combined Register Values
RPR0521RS_MODE_CONTROL_VAL = const(RPR0521RS_MODE_CONTROL_MEASTIME_100_100MS |
                                   RPR0521RS_MODE_CONTROL_PS_EN |
                                   RPR0521RS_MODE_CONTROL_ALS_EN)
RPR0521RS_ALS_PS_CONTROL_VAL = const(RPR0521RS_ALS_PS_CONTROL_DATA0_GAIN_X1 |
                                     RPR0521RS_ALS_PS_CONTROL_DATA1_GAIN_X1 |
                                     RPR0521RS_ALS_PS_CONTROL_LED_CURRENT_100MA)
RPR0521RS_PS_CONTROL_VAL = const(RPR0521RS_PS_CONTROL_PS_GAINX1)

# Proximity Threshold
RPR0521RS_NEAR_THRESH = const(1000)  # example value
RPR0521RS_FAR_VAL = const(0)
RPR0521RS_NEAR_VAL = const(1)

# Error Value
RPR0521RS_ERROR = const(-1)

# ALS Gain Table
ALS_GAIN_TABLE = [1, 2, 64, 128]

# ALS Measurement Time Table (in ms)
ALS_MEASUREMENT_TIME_TABLE = [0, 0, 0, 0, 0, 100, 100, 100, 100, 100, 400, 400, 50, 0, 0, 0]


class RPR0521RS(Sensor):
    # Register definitions
    _system_control = RegisterStruct(RPR0521RS_SYSTEM_CONTROL, ">B")
    _mode_control = RegisterStruct(RPR0521RS_MODE_CONTROL, ">B")
    _als_ps_control = RegisterStruct(RPR0521RS_ALS_PS_CONTROL, ">B")
    _ps_control = RegisterStruct(RPR0521RS_PS_CONTROL, ">B")
    _manufact_id = RegisterStruct(RPR0521RS_MANUFACT_ID, ">B")

    def __init__(self, i2c, address=RPR0521RS_DEVICE_ADDRESS):
        """Initialize the RPR-0521RS sensor."""
        super().__init__(i2c, "RPR0521RS", address=address)
        self.i2c = i2c
        self.address = address

        # Verify device
        try:
            part_id = self._system_control & 0x3F
            if part_id != RPR0521RS_PART_ID_VAL:
                raise RuntimeError(f"Failed to find RPR-0521RS! Part ID: 0x{part_id:02X}")

            manufact_id = self._manufact_id
            if manufact_id != RPR0521RS_MANUFACT_ID_VAL:
                raise RuntimeError(f"Failed to find RPR-0521RS! Manufacturer ID: 0x{manufact_id:02X}")
        except Exception as e:
            raise RuntimeError("Failed to communicate with RPR-0521RS sensor") from e

        # Initialize device
        self._init_sensor()

    def start(self):
        """Initialize the sensor (already done in __init__)"""
        # Initialization already done in __init__
        return True

    def read(self):
        """Read sensor data and return as dictionary"""
        self.data = {
            'ambient_light': self.ambient_light,
            'proximity': self.proximity,
            'illumination': self.illumination,
            'infrared_illumination': self.infrared_illumination,
        }
        return self.data

    @staticmethod
    def display(data):
        """Format sensor data for display
        
        Parameters:
            data: Dictionary containing sensor readings
            
        Returns:
            Formatted string for display
        """
        if not data:
            return "RPR0521RS Ambient Light/Proximity Sensor: No data available"

        result = "RPR0521RS Ambient Light/Proximity Sensor:\n"
        if 'ambient_light' in data:
            result += f"  Ambient Light: {data['ambient_light']:.2f} lx\n"
        if 'proximity' in data:
            result += f"  Proximity: {data['proximity']}\n"
        if 'illumination' in data:
            result += f"  Illumination: {data['illumination']:.2f} lx\n"
        if 'infrared_illumination' in data:
            result += f"  Infrared Illumination: {data['infrared_illumination']:.2f} lx"

        return result

    def _init_sensor(self):
        """Initialize the sensor with default settings."""
        # Set ALS/PS control register
        self._als_ps_control = RPR0521RS_ALS_PS_CONTROL_VAL

        # Read current PS control register
        ps_control_val = self._ps_control

        # Set PS control register
        self._ps_control = ps_control_val | RPR0521RS_PS_CONTROL_VAL

        # Set mode control register
        self._mode_control = RPR0521RS_MODE_CONTROL_VAL

        # Set gain values for calculations
        als_ps_control_val = self._als_ps_control
        data0_gain_index = (als_ps_control_val >> 4) & 0x03
        data1_gain_index = (als_ps_control_val >> 2) & 0x03

        self._als_data0_gain = ALS_GAIN_TABLE[data0_gain_index]
        self._als_data1_gain = ALS_GAIN_TABLE[data1_gain_index]

        # Set measurement time
        mode_control_val = self._mode_control
        meas_time_index = mode_control_val & 0x0F
        self._als_measure_time = ALS_MEASUREMENT_TIME_TABLE[meas_time_index]

        # Wait for measurements to stabilize
        time.sleep(0.1)

    def get_raw_data(self):
        """Get raw sensor data.

        :return: Tuple containing (ps_data, als_data0, als_data1)
        :rtype: tuple
        """
        # Read 6 bytes starting from PS_DATA_LSB register
        buffer = bytearray(6)
        self.i2c.readfrom_mem_into(self.address, RPR0521RS_PS_DATA_LSB, buffer)

        # Parse the data
        ps_data = buffer[0] | (buffer[1] << 8)
        als_data0 = buffer[2] | (buffer[3] << 8)
        als_data1 = buffer[4] | (buffer[5] << 8)

        return (ps_data, als_data0, als_data1)

    def get_adjusted_data(self):
        """Get gain and measurement time adjusted data.

        :return: Tuple containing (ps_data, adjusted_data0, adjusted_data1)
        :rtype: tuple
        """
        ps_data, data0, data1 = self.get_raw_data()

        # Handle overflow for 50ms measurement time
        if self._als_measure_time == 50:
            if (data0 & 0x8000) == 0x8000:
                data0 = 0x7FFF
            if (data1 & 0x8000) == 0x8000:
                data1 = 0x7FFF

        # Apply gain and measurement time compensation
        adjusted_data0 = (float(data0) * (100 / self._als_measure_time)) / self._als_data0_gain
        adjusted_data1 = (float(data1) * (100 / self._als_measure_time)) / self._als_data1_gain

        return (ps_data, adjusted_data0, adjusted_data1)

    @property
    def proximity(self):
        """The current proximity value.

        :return: Proximity reading
        :rtype: int
        """
        ps_data, _, _ = self.get_raw_data()
        return ps_data

    @property
    def illumination(self):
        """The current illumination value (visible light + infrared).

        :return: Illumination reading (data0 adjusted for gain and measurement time)
        :rtype: float
        """
        _, adjusted_data0, _ = self.get_adjusted_data()
        return adjusted_data0

    @property
    def infrared_illumination(self):
        """The current infrared illumination value.

        :return: Infrared illumination reading (data1 adjusted for gain and measurement time)
        :rtype: float
        """
        _, _, adjusted_data1 = self.get_adjusted_data()
        return adjusted_data1

    @property
    def ambient_light(self):
        """The current ambient light value in lux.

        :return: Ambient light reading in lux
        :rtype: float
        """
        _, als_data0, als_data1 = self.get_raw_data()

        # Convert raw values to lux
        lux = self._convert_to_lux(als_data0, als_data1)
        return lux

    def check_proximity_status(self):
        """Check if an object is near or far based on threshold.

        :return: RPR0521RS_NEAR_VAL (1) if object is near, RPR0521RS_FAR_VAL (0) if object is far
        :rtype: int
        """
        ps_data = self.proximity
        if ps_data >= RPR0521RS_NEAR_THRESH:
            return RPR0521RS_NEAR_VAL
        else:
            return RPR0521RS_FAR_VAL

    def _convert_to_lux(self, data0, data1):
        """Convert raw sensor data to lux value.

        :param int data0: Raw data from channel 0
        :param int data1: Raw data from channel 1
        :return: Ambient light in lux
        :rtype: float
        """
        # Check if gain and measurement time are valid
        if (self._als_data0_gain == 0 or
                self._als_data1_gain == 0 or
                self._als_measure_time == 0):
            return RPR0521RS_ERROR

        # Handle overflow for 50ms measurement time
        if self._als_measure_time == 50:
            if (data0 & 0x8000) == 0x8000:
                data0 = 0x7FFF
            if (data1 & 0x8000) == 0x8000:
                data1 = 0x7FFF

        # Apply gain and measurement time compensation
        d0 = (float(data0) * (100 / self._als_measure_time)) / self._als_data0_gain
        d1 = (float(data1) * (100 / self._als_measure_time)) / self._als_data1_gain

        # Avoid division by zero
        if d0 == 0:
            return 0

        # Calculate lux based on ratio
        ratio = d1 / d0

        # Apply appropriate formula based on ratio
        if ratio < 0.595:
            lux = (1.682 * d0 - 1.877 * d1)
        elif ratio < 1.015:
            lux = (0.644 * d0 - 0.132 * d1)
        elif ratio < 1.352:
            lux = (0.756 * d0 - 0.243 * d1)
        elif ratio < 3.053:
            lux = (0.766 * d0 - 0.25 * d1)
        else:
            lux = 0

        return lux

    def reset(self):
        """Reset the sensor to default settings."""
        # Reset sensor by re-initializing
        self._init_sensor()


if __name__ == "__main__":
    """Example usage of the RPR-0521RS driver."""
    from machine import I2C, Pin

    print("=== RPR-0521RS Sensor Example ===")

    # Initialize I2C
    i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=400000)

    try:
        # Initialize RPR-0521RS at default address 0x38
        sensor = RPR0521RS(i2c)
        print("RPR-0521RS sensor initialized successfully!")
    except Exception as e:
        print(f"Failed to initialize RPR-0521RS sensor: {e}")
        raise SystemExit

    print("\n=== RPR-0521RS Sensor Readings ===")
    print("{:^15} | {:^15} | {:^15} | {:^15}".format(
        "Proximity", "Light (lux)", "Illumination", "IR Illumination"))
    print("-" * 67)

    try:
        for _ in range(100):  # Read samples
            proximity = sensor.proximity
            light = sensor.ambient_light
            illumination = sensor.illumination
            ir_illumination = sensor.infrared_illumination

            proximity_status = "NEAR" if sensor.check_proximity_status() == RPR0521RS_NEAR_VAL else "FAR"

            print("{:>8} ({:>4}) | {:>13.2f} | {:>13.2f} | {:>13.2f}".format(
                proximity, proximity_status, light, illumination, ir_illumination
            ))

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    print("Program finished.")
