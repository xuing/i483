# SPDX-FileCopyrightText: Copyright (c) 2023 Jose D. Montoya
# SPDX-FileCopyrightText: Copyright (c) 2025 XU pengfei
#
# SPDX-License-Identifier: MIT

import time
import math
import struct
from micropython import const

from i2c_helpers import RegisterStruct, CBits

# Register Addresses
_DEVICE_ID = const(0x0D)
_PRS_CFG = const(0x06)
_TMP_CFG = const(0x07)
_MEAS_CFG = const(0x08)
_CFGREG = const(0x09)
_RESET = const(0x0C)
_TMPCOEFSRCE = const(0x28)  # Temperature calibration src

# DPS310 Measurement Constants
# Oversampling Rates
SAMPLE_PER_SECOND_1 = const(0b000)  # 1 time (Pressure Low Precision)
SAMPLE_PER_SECOND_2 = const(0b001)  # 2 times (Pressure Low Power)
SAMPLE_PER_SECOND_4 = const(0b010)  # 4 times
SAMPLE_PER_SECOND_8 = const(0b011)  # 8 times
SAMPLE_PER_SECOND_16 = const(0b100)  # 16 times (Pressure Standard)
SAMPLE_PER_SECOND_32 = const(0b101)  # 32 times
SAMPLE_PER_SECOND_64 = const(0b110)  # 64 times (Pressure High Precision)
SAMPLE_PER_SECOND_128 = const(0b111)  # 128 times

# Sample Rates
RATE_1_HZ = const(0b000)
RATE_2_HZ = const(0b001)
RATE_4_HZ = const(0b010)
RATE_8_HZ = const(0b011)
RATE_16_HZ = const(0b100)
RATE_32_HZ = const(0b101)
RATE_64_HZ = const(0b110)
RATE_128_HZ = const(0b111)

# Sensor Operating Modes
IDLE = const(0b000)  # Standby mode
ONE_PRESSURE = const(0b001)  # Single pressure measurement
ONE_TEMPERATURE = const(0b010)  # Single temperature measurement
CONT_PRESSURE = const(0b101)  # Continuous pressure measurement
CONT_TEMP = const(0b110)  # Continuous temperature measurement
CONT_PRESTEMP = const(0b111)  # Continuous pressure and temperature measurement

# Valid options lists
OVERSAMPLE_OPTIONS = (
    SAMPLE_PER_SECOND_1,
    SAMPLE_PER_SECOND_2,
    SAMPLE_PER_SECOND_4,
    SAMPLE_PER_SECOND_8,
    SAMPLE_PER_SECOND_16,
    SAMPLE_PER_SECOND_32,
    SAMPLE_PER_SECOND_64,
    SAMPLE_PER_SECOND_128,
)

RATE_OPTIONS = (
    RATE_1_HZ,
    RATE_2_HZ,
    RATE_4_HZ,
    RATE_8_HZ,
    RATE_16_HZ,
    RATE_32_HZ,
    RATE_64_HZ,
    RATE_128_HZ,
)

MODE_OPTIONS = (
    IDLE,
    ONE_PRESSURE,
    ONE_TEMPERATURE,
    CONT_PRESSURE,
    CONT_TEMP,
    CONT_PRESTEMP,
)

# Human-readable names for constants
OVERSAMPLE_NAMES = (
    "SAMPLE_PER_SECOND_1",
    "SAMPLE_PER_SECOND_2",
    "SAMPLE_PER_SECOND_4",
    "SAMPLE_PER_SECOND_8",
    "SAMPLE_PER_SECOND_16",
    "SAMPLE_PER_SECOND_32",
    "SAMPLE_PER_SECOND_64",
    "SAMPLE_PER_SECOND_128",
)

RATE_NAMES = (
    "RATE_1_HZ",
    "RATE_2_HZ",
    "RATE_4_HZ",
    "RATE_8_HZ",
    "RATE_16_HZ",
    "RATE_32_HZ",
    "RATE_64_HZ",
    "RATE_128_HZ",
)

MODE_NAMES = {
    IDLE: "IDLE",
    ONE_PRESSURE: "ONE_PRESSURE",
    ONE_TEMPERATURE: "ONE_TEMPERATURE",
    CONT_PRESSURE: "CONT_PRESSURE",
    CONT_TEMP: "CONT_TEMP",
    CONT_PRESTEMP: "CONT_PRESTEMP",
}


class DPS310:
    """Driver for the DPS310 Barometric Sensor.

    :param ~machine.I2C i2c: The I2C bus the DPS310 is connected to.
    :param int address: The I2C device address. Defaults to :const:`0x77`

    :raises RuntimeError: if the sensor is not found

    **Quickstart: Importing and using the device**

    Here is an example of using the :class:`DPS310` class.
    First you will need to import the libraries to use the sensor

    .. code-block:: python

        from machine import Pin, I2C
        import dps310 as dps310

    Once this is done you can define your `machine.I2C` object and define your sensor object

    .. code-block:: python

        i2c = I2C(1, sda=Pin(2), scl=Pin(3))
        dps = dps310.DPS310(i2c)

    Now you have access to the :attr:`pressure`, :attr:`temperature`, and :attr:`altitude` attributes

    .. code-block:: python

        press = dps.pressure
        temp = dps.temperature
        alt = dps.altitude
    """

    # Register definitions
    _device_id = RegisterStruct(_DEVICE_ID, ">B")
    _reset_register = RegisterStruct(_RESET, ">B")
    _press_conf_reg = RegisterStruct(_PRS_CFG, ">B")
    _temp_conf_reg = RegisterStruct(_TMP_CFG, ">B")
    _sensor_operation_mode = RegisterStruct(_MEAS_CFG, ">B")

    # Register Bit definitions
    # Pressure Configuration (0x06)
    _pressure_oversample = CBits(4, _PRS_CFG, 0)
    _pressure_rate = CBits(3, _PRS_CFG, 4)

    # Temperature Configuration (0x07)
    _temperature_oversample = CBits(4, _TMP_CFG, 0)
    _temperature_rate = CBits(3, _TMP_CFG, 4)
    _temperature_external_source = CBits(1, _TMP_CFG, 7)

    # Sensor Operating Mode and Status (0x08)
    _sensor_mode = CBits(3, _MEAS_CFG, 0)
    _pressure_ready = CBits(1, _MEAS_CFG, 4)
    _temp_ready = CBits(1, _MEAS_CFG, 5)
    _sensor_ready = CBits(1, _MEAS_CFG, 6)
    _coefficients_ready = CBits(1, _MEAS_CFG, 7)

    # Sensor interrupts (0x09)
    _t_shift = CBits(1, _CFGREG, 3)
    _p_shift = CBits(1, _CFGREG, 2)

    # Raw measurement data registers
    _raw_pressure = CBits(24, 0x00, 0, 3, False)
    _raw_temperature = CBits(24, 0x03, 0, 3, False)

    # Calibration source control
    _calib_coeff_temp_src_bit = CBits(1, _TMPCOEFSRCE, 7)

    # Temperature correction registers
    _reg0e = CBits(8, 0x0E, 0)
    _reg0f = CBits(8, 0x0F, 0)
    _reg62 = CBits(8, 0x62, 0)

    # Reset control
    _soft_reset = CBits(4, 0x0C, 0)

    def __init__(self, i2c, address=0x77) -> None:
        """Initialize the DPS310 sensor."""
        self._i2c = i2c
        self._address = address
        self._sea_level_pressure = 1013.25  # Default sea level pressure in hPa

        # Check if device is present
        if self._device_id != 0x10:
            raise RuntimeError("Failed to find the DPS310 sensor!")

        # Scaling factors for different oversample rates
        self._oversample_scalefactor = (
            524288.0,  # OS = 1
            1572864.0,  # OS = 2
            3670016.0,  # OS = 4
            7864320.0,  # OS = 8
            253952.0,  # OS = 16
            516096.0,  # OS = 32
            1040384.0,  # OS = 64
            2088960.0,  # OS = 128
        )

        # Initialize sensor
        self._correct_temp()
        self._read_calibration()
        self._temp_measurement_src_bit = self._calib_coeff_temp_src_bit

        # Configure default settings
        self.pressure_oversample = SAMPLE_PER_SECOND_64
        self.temperature_oversample = SAMPLE_PER_SECOND_64
        self._sensor_mode = CONT_PRESTEMP

        # Wait for initial measurements
        self._wait_temperature_ready()
        self._wait_pressure_ready()

    @property
    def pressure_oversample(self) -> str:
        """
        Pressure Oversample setting. Higher values provide better precision but increase
        measurement time and current consumption.

        Returns one of the following strings:
        - SAMPLE_PER_SECOND_1: 1 time (Pressure Low Precision)
        - SAMPLE_PER_SECOND_2: 2 times (Pressure Low Power)
        - SAMPLE_PER_SECOND_4: 4 times
        - SAMPLE_PER_SECOND_8: 8 times
        - SAMPLE_PER_SECOND_16: 16 times (Pressure Standard)
        - SAMPLE_PER_SECOND_32: 32 times
        - SAMPLE_PER_SECOND_64: 64 times (Pressure High Precision)
        - SAMPLE_PER_SECOND_128: 128 times
        """
        return OVERSAMPLE_NAMES[self._pressure_oversample]

    @pressure_oversample.setter
    def pressure_oversample(self, value: int) -> None:
        """Set pressure oversample value."""
        if value not in OVERSAMPLE_OPTIONS:
            raise ValueError("Value must be a valid oversample setting")
        self._pressure_oversample = value
        self._p_shift = value > SAMPLE_PER_SECOND_8
        self._pressure_scale = self._oversample_scalefactor[value]

    @property
    def pressure_rate(self) -> str:
        """
        Pressure measurement rate.

        Returns one of: RATE_1_HZ, RATE_2_HZ, RATE_4_HZ, RATE_8_HZ,
        RATE_16_HZ, RATE_32_HZ, RATE_64_HZ, or RATE_128_HZ
        """
        return RATE_NAMES[self._pressure_rate]

    @pressure_rate.setter
    def pressure_rate(self, value: int) -> None:
        """Set pressure measurement rate."""
        if value not in RATE_OPTIONS:
            raise ValueError("Value must be a valid rate setting")
        self._pressure_rate = value

    @property
    def temperature_oversample(self) -> str:
        """
        Temperature Oversample setting. Higher values provide better precision but increase
        measurement time and current consumption.

        Returns one of the following strings:
        - SAMPLE_PER_SECOND_1: 1 time
        - SAMPLE_PER_SECOND_2: 2 times
        - SAMPLE_PER_SECOND_4: 4 times
        - SAMPLE_PER_SECOND_8: 8 times
        - SAMPLE_PER_SECOND_16: 16 times
        - SAMPLE_PER_SECOND_32: 32 times
        - SAMPLE_PER_SECOND_64: 64 times
        - SAMPLE_PER_SECOND_128: 128 times
        """
        return OVERSAMPLE_NAMES[self._temperature_oversample]

    @temperature_oversample.setter
    def temperature_oversample(self, value: int) -> None:
        """Set temperature oversample value."""
        if value not in OVERSAMPLE_OPTIONS:
            raise ValueError("Value must be a valid oversample setting")
        self._temperature_oversample = value
        self._temp_scale = self._oversample_scalefactor[value]
        self._t_shift = value > SAMPLE_PER_SECOND_8

    @property
    def temperature_rate(self) -> str:
        """
        Temperature measurement rate.

        Returns one of: RATE_1_HZ, RATE_2_HZ, RATE_4_HZ, RATE_8_HZ,
        RATE_16_HZ, RATE_32_HZ, RATE_64_HZ, or RATE_128_HZ
        """
        return RATE_NAMES[self._temperature_rate]

    @temperature_rate.setter
    def temperature_rate(self, value: int) -> None:
        """Set temperature measurement rate."""
        if value not in RATE_OPTIONS:
            raise ValueError("Value must be a valid rate setting")
        self._temperature_rate = value

    @property
    def mode(self) -> str:
        """
        Sensor operating mode.

        Returns one of:
        - IDLE: Standby mode
        - ONE_PRESSURE: Single pressure measurement
        - ONE_TEMPERATURE: Single temperature measurement
        - CONT_PRESSURE: Continuous pressure measurement
        - CONT_TEMP: Continuous temperature measurement
        - CONT_PRESTEMP: Continuous pressure and temperature measurement
        """
        return MODE_NAMES[self._sensor_mode]

    @mode.setter
    def mode(self, value: int) -> None:
        """Set sensor operating mode."""
        if value not in MODE_OPTIONS:
            raise ValueError("Value must be a valid mode setting")
        self._sensor_mode = value

    @staticmethod
    def _twos_complement(val: int, bits: int) -> int:
        """Convert two's complement value to signed integer."""
        if val & (1 << (bits - 1)):
            val -= 1 << bits
        return val

    def _wait_pressure_ready(self) -> None:
        """Wait until a pressure measurement is available."""
        if self.mode in (IDLE, ONE_TEMPERATURE, CONT_TEMP):
            raise RuntimeError(
                "Current sensor mode doesn't support pressure measurements"
            )
        while self._pressure_ready is False:
            time.sleep(0.001)

    def _wait_temperature_ready(self) -> None:
        """Wait until a temperature measurement is available."""
        if self.mode in (IDLE, ONE_PRESSURE, CONT_PRESSURE):
            raise RuntimeError(
                "Current sensor mode doesn't support temperature measurements"
            )

        while self._temp_ready is False:
            time.sleep(0.001)

    def _correct_temp(self) -> None:
        """Correct temperature readings on ICs with a fuse bit problem."""
        self._reg0e = 0xA5
        self._reg0f = 0x96
        self._reg62 = 0x02
        self._reg0e = 0
        self._reg0f = 0

        # Discard initial temperature reading
        _ = self._raw_temperature

    def _read_calibration(self) -> None:
        """
        Read the calibration data from the sensor
        """
        while not self._coefficients_ready:
            time.sleep(0.001)

        # Read all coefficient registers at once for efficiency
        coeffs = bytearray(18)
        for offset in range(18):
            register = 0x10 + offset
            coeffs[offset] = struct.unpack(
                "B", self._i2c.readfrom_mem(self._address, register, 1)
            )[0]

        # Process coefficients according to datasheet formulas
        self._c0 = (coeffs[0] << 4) | ((coeffs[1] >> 4) & 0x0F)
        self._c0 = self._twos_complement(self._c0, 12)

        self._c1 = self._twos_complement(((coeffs[1] & 0x0F) << 8) | coeffs[2], 12)

        self._c00 = (coeffs[3] << 12) | (coeffs[4] << 4) | ((coeffs[5] >> 4) & 0x0F)
        self._c00 = self._twos_complement(self._c00, 20)

        self._c10 = ((coeffs[5] & 0x0F) << 16) | (coeffs[6] << 8) | coeffs[7]
        self._c10 = self._twos_complement(self._c10, 20)

        self._c01 = self._twos_complement((coeffs[8] << 8) | coeffs[9], 16)
        self._c11 = self._twos_complement((coeffs[10] << 8) | coeffs[11], 16)
        self._c20 = self._twos_complement((coeffs[12] << 8) | coeffs[13], 16)
        self._c21 = self._twos_complement((coeffs[14] << 8) | coeffs[15], 16)
        self._c30 = self._twos_complement((coeffs[16] << 8) | coeffs[17], 16)

    @property
    def temperature(self) -> float:
        """The current temperature reading in Celsius."""
        scaled_rawtemp = self._raw_temperature / self._temp_scale
        temp = scaled_rawtemp * self._c1 + self._c0 / 2.0
        return temp

    @property
    def pressure(self) -> float:
        """Returns the current pressure reading in hectoPascals (hPa)."""
        # Get raw readings
        raw_temperature = self._twos_complement(self._raw_temperature, 24)
        raw_pressure = self._twos_complement(self._raw_pressure, 24)

        # Scale according to current settings
        scaled_rawtemp = raw_temperature / self._temp_scale
        scaled_rawpres = raw_pressure / self._pressure_scale

        # Apply calibration compensation formula from datasheet
        pres_calc = (
                self._c00
                + scaled_rawpres
                * (self._c10 + scaled_rawpres * (self._c20 + scaled_rawpres * self._c30))
                + scaled_rawtemp
                * (self._c01 + scaled_rawpres * (self._c11 + scaled_rawpres * self._c21))
        )

        # Convert to hPa
        return pres_calc / 100

    @property
    def altitude(self) -> float:
        """
        Altitude in meters based on pressure and sea-level reference.
        Uses the barometric formula for altitude calculation.
        """
        try:
            ratio = self.pressure / self._sea_level_pressure
            if ratio <= 0:
                return float('nan')
            # Approximation of the international barometric formula
            return 44330.0 * (1.0 - math.pow(ratio, 0.1903))
        except Exception as e:
            print(f"Altitude calculation error: {e}")
            return float('nan')

    @altitude.setter
    def altitude(self, value: float) -> None:
        """Set sea level pressure based on the current pressure and known altitude."""
        self.sea_level_pressure = self.pressure / (1.0 - value / 44330.0) ** 5.255

    @property
    def sea_level_pressure(self) -> float:
        """The local sea level pressure in hectoPascals (aka millibars)."""
        return self._sea_level_pressure

    @sea_level_pressure.setter
    def sea_level_pressure(self, value: float) -> None:
        """Set sea level pressure reference in hectoPascals."""
        if value <= 0:
            raise ValueError("Sea level pressure must be positive")
        self._sea_level_pressure = value

    def reset(self) -> None:
        """Perform a soft reset of the sensor."""
        self._soft_reset = 0x09  # Value from datasheet for soft reset
        time.sleep(0.1)  # Wait for reset to complete

        # Re-initialize
        self._correct_temp()
        self._read_calibration()
        self._temp_measurement_src_bit = self._calib_coeff_temp_src_bit

        # Restore default settings
        self.pressure_oversample = SAMPLE_PER_SECOND_64
        self.temperature_oversample = SAMPLE_PER_SECOND_64
        self._sensor_mode = CONT_PRESTEMP

        # Wait for measurements to be available
        self._wait_temperature_ready()
        self._wait_pressure_ready()


if __name__ == "__main__":
    """Example usage of the DPS310 driver."""
    from machine import I2C, Pin

    print("=== DPS310 Sensor Example ===")

    # Initialize I2C
    i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=400000)

    # Scan I2C bus
    devices = i2c.scan()
    if devices:
        print(f"Found {len(devices)} devices on I2C bus:")
        for device in devices:
            print(f"  Device address: 0x{device:02X}")
    else:
        print("No devices found on I2C bus, please check connections")
        raise SystemExit

    try:
        # Initialize DPS310 at default address 0x77
        sensor = DPS310(i2c, address=0x77)
        print("DPS310 sensor initialized successfully!")
    except Exception as e:
        print(f"Failed to initialize DPS310 sensor: {e}")
        raise SystemExit

    print("\n=== DPS310 Sensor Readings ===")
    print("{:^12} | {:^12} | {:^10}".format("Temp (°C)", "Press (hPa)", "Alt (m)"))
    print("-" * 40)

    try:
        for _ in range(10):  # Read 10 samples
            temperature = sensor.temperature
            pressure = sensor.pressure
            altitude = sensor.altitude

            print("{:>10.2f} °C | {:>10.2f} hPa | {:>8.2f} m".format(
                temperature, pressure, altitude
            ))

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    # Stop sensor measurements
    sensor.mode = IDLE
    print("Sensor set to IDLE. Program finished.")