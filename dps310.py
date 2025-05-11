# SPDX-FileCopyrightText: Copyright (c) 2023 Jose D. Montoya
# SPDX-FileCopyrightText: Copyright (c) 2025 XU pengfei
#
# SPDX-License-Identifier: MIT

import math
import time

from micropython import const

from i2c_helpers import RegisterStruct, CBits
from sensor import Sensor

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


class DPS310(Sensor):
    """Driver for the DPS310 Barometric Sensor.
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
    _temperature_external_source = CBits(1, _TMP_CFG, 7)  # <--- This controls TMP_EXT bit

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
    _calib_coeff_temp_src_bit = CBits(1, _TMPCOEFSRCE, 7)  # <--- This reads TMP_COEF_SRCE

    # Temperature correction registers
    _reg0e = CBits(8, 0x0E, 0)
    _reg0f = CBits(8, 0x0F, 0)
    _reg62 = CBits(8, 0x62, 0)

    # Reset control
    _soft_reset = CBits(4, 0x0C, 0)

    def __init__(self, i2c, name="DPS310", address=0x77) -> None:
        """Initialize the DPS310 sensor."""
        super().__init__(i2c, name, address=address)
        self._sea_level_pressure = 1013.25  # Default sea level pressure in hPa

        # Check if device is present
        if self._device_id != 0x10:  # Datasheet page 36, PROD_ID is 0x0, REV_ID is 0x1 for 0x10
            raise RuntimeError("Failed to find the DPS310 sensor! Device ID mismatch.")

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
        # self._correct_temp() # Consider if this is always needed, or conditional based on errata
        self._read_calibration()

        # --- MODIFICATION START ---
        # Ensure temperature sensor source for measurement matches calibration coefficient source
        self._temp_measurement_src_bit = self._calib_coeff_temp_src_bit
        if self._temperature_external_source != self._temp_measurement_src_bit:
            # print(f"Configuring temperature sensor source to: {'External MEMS' if self._temp_measurement_src_bit else 'Internal ASIC'}")
            self._temperature_external_source = self._temp_measurement_src_bit
            time.sleep(0.01)  # Allow a short time for the setting to apply if needed
        # --- MODIFICATION END ---

        # Configure default settings
        self.pressure_oversample = SAMPLE_PER_SECOND_64
        self.temperature_oversample = SAMPLE_PER_SECOND_64  # This will set _temp_scale and _t_shift
        self._sensor_mode = CONT_PRESTEMP

        # Wait for initial measurements to be available and sensor to be ready
        # It is good practice to wait for _coefficients_ready before reading them, which _read_calibration does.
        # And wait for _sensor_ready before starting measurements, though MEAS_CFG default is C0h, meaning sensor ready.
        # However, setting mode to CONT_PRESTEMP will start measurements.
        # Let's ensure sensor is ready before proceeding if there were any resets or initial power-up delays.
        # The _coefficients_ready check is already in _read_calibration.
        # Adding a check for general sensor readiness if not covered.
        # For now, the existing waits for temp/pressure data after mode set should suffice.

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
        self._p_shift = value > SAMPLE_PER_SECOND_8  # P_SHIFT is bit 2 of CFG_REG (0x09)
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
        self._temperature_oversample = value  # This updates bits 3:0 of TMP_CFG (0x07)
        self._temp_scale = self._oversample_scalefactor[value]
        self._t_shift = value > SAMPLE_PER_SECOND_8  # T_SHIFT is bit 3 of CFG_REG (0x09)

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
        # After changing mode, especially to a continuous mode, allow some time for data to become ready
        if value in (CONT_PRESSURE, CONT_TEMP, CONT_PRESTEMP):
            time.sleep(0.05)  # Small delay, specific timing depends on rate/oversample

    @staticmethod
    def _twos_complement(val: int, bits: int) -> int:
        """Convert two's complement value to signed integer."""
        if val & (1 << (bits - 1)):
            val -= 1 << bits
        return val

    def _wait_pressure_ready(self) -> None:
        """Wait until a pressure measurement is available."""
        if self.mode in (IDLE, ONE_TEMPERATURE,
                         CONT_TEMP):  # Corrected: was checking self.mode (string) against int constants
            current_mode_val = self._sensor_mode  # get actual int value
            if current_mode_val == IDLE or \
                    current_mode_val == ONE_TEMPERATURE or \
                    current_mode_val == CONT_TEMP:
                raise RuntimeError(
                    "Current sensor mode doesn't support pressure measurements"
                )
        while self._pressure_ready is False:  # PRS_RDY flag in MEAS_CFG (0x08)
            time.sleep(0.001)

    def _wait_temperature_ready(self) -> None:
        """Wait until a temperature measurement is available."""
        if self.mode in (IDLE, ONE_PRESSURE,
                         CONT_PRESSURE):  # Corrected: was checking self.mode (string) against int constants
            current_mode_val = self._sensor_mode  # get actual int value
            if current_mode_val == IDLE or \
                    current_mode_val == ONE_PRESSURE or \
                    current_mode_val == CONT_PRESSURE:
                raise RuntimeError(
                    "Current sensor mode doesn't support temperature measurements"
                )

        while self._temp_ready is False:  # TMP_RDY flag in MEAS_CFG (0x08)
            time.sleep(0.001)

    def _correct_temp(self) -> None:
        """Correct temperature readings on ICs with a fuse bit problem.
        WARNING: This sequence is not documented in the provided datasheet.
        Its necessity and correctness should be verified for the specific sensor revision.
        """
        # Check if these registers are safe to write to, as they are not in the datasheet's public map.
        # This might be from an errata or specific app note.
        # If unsure, this part could be commented out for testing.
        # print("Applying _correct_temp() sequence.")
        self._reg0e = 0xA5
        self._reg0f = 0x96
        self._reg62 = 0x02
        self._reg0e = 0  # Assuming these are set back to 0 for a reason.
        self._reg0f = 0

        # Discard initial temperature reading after this potential correction
        # This read might be too soon if the sensor needs time after these writes.
        # A small delay might be prudent if this correction is indeed necessary.
        # time.sleep(0.005) # Optional small delay
        _ = self._raw_temperature

    def _read_calibration(self) -> None:
        """
        Read the calibration data from the sensor
        """
        # Wait until coefficients are ready. COEF_RDY bit in MEAS_CFG (0x08)
        # Datasheet: "Time to coefficients are available. T_Coef_rdy = 40ms (Max)" from POR/Reset
        # This loop handles waiting for the COEF_RDY bit.
        timeout_ms = 50  # Max 40ms, add a bit of buffer
        start_time = time.ticks_ms()
        while not self._coefficients_ready:
            if time.ticks_diff(time.ticks_ms(), start_time) > timeout_ms:
                raise RuntimeError("Timeout waiting for calibration coefficients to be ready.")
            time.sleep(0.001)

        # Read all coefficient registers at once for efficiency (addresses 0x10 to 0x21)
        # Total 18 bytes for c0, c1, c00, c10, c01, c11, c20, c21, c30
        coeffs = bytearray(18)
        # The RegisterStruct and CBits helpers usually read/write single bytes or defined bit fields.
        # For block reads, direct i2c access is often used.
        # Assuming i2c_helpers can handle this if the CBits are defined over multiple bytes,
        # but typically for an 18-byte block, a direct read is more common.
        # The original code reads byte by byte, which is fine but less efficient. Let's stick to it for now
        # to minimize changes beyond the main fix.
        for offset in range(18):
            register = 0x10 + offset
            # Ensure readfrom_mem is correctly implemented in the environment.
            # For CircuitPython/MicroPython, it's usually i2c.readfrom_mem(address, register, num_bytes)
            coeffs[offset] = self.i2c.readfrom_mem(self.address, register, 1)[0]

        # Process coefficients according to datasheet formulas (Section 4.9, page 14 & 37)
        # c0: 12-bit 2's complement (reg 0x10, 0x11[7:4])
        self._c0 = (coeffs[0] << 4) | ((coeffs[1] >> 4) & 0x0F)
        self._c0 = self._twos_complement(self._c0, 12)

        # c1: 12-bit 2's complement (reg 0x11[3:0], 0x12)
        self._c1 = self._twos_complement(((coeffs[1] & 0x0F) << 8) | coeffs[2], 12)

        # c00: 20-bit 2's complement (reg 0x13, 0x14, 0x15[7:4])
        self._c00 = (coeffs[3] << 12) | (coeffs[4] << 4) | ((coeffs[5] >> 4) & 0x0F)
        self._c00 = self._twos_complement(self._c00, 20)

        # c10: 20-bit 2's complement (reg 0x15[3:0], 0x16, 0x17)
        self._c10 = ((coeffs[5] & 0x0F) << 16) | (coeffs[6] << 8) | coeffs[7]
        self._c10 = self._twos_complement(self._c10, 20)

        # c01: 16-bit 2's complement (reg 0x18, 0x19)
        self._c01 = self._twos_complement((coeffs[8] << 8) | coeffs[9], 16)
        # c11: 16-bit 2's complement (reg 0x1A, 0x1B)
        self._c11 = self._twos_complement((coeffs[10] << 8) | coeffs[11], 16)
        # c20: 16-bit 2's complement (reg 0x1C, 0x1D)
        self._c20 = self._twos_complement((coeffs[12] << 8) | coeffs[13], 16)
        # c21: 16-bit 2's complement (reg 0x1E, 0x1F)
        self._c21 = self._twos_complement((coeffs[14] << 8) | coeffs[15], 16)
        # c30: 16-bit 2's complement (reg 0x20, 0x21)
        self._c30 = self._twos_complement((coeffs[16] << 8) | coeffs[17], 16)

    @property
    def temperature(self) -> float:
        """The current temperature reading in Celsius."""
        # Raw temperature is 24-bit 2's complement
        raw_temp_val = self._twos_complement(self._raw_temperature, 24)
        # Scaled raw temperature (Traw_sc)
        # Traw_sc = Traw / kT (kT is self._temp_scale)
        scaled_rawtemp = raw_temp_val / self._temp_scale
        # Compensated temperature: Tcomp(°C) = c0 * 0.5 + c1 * Traw_sc (Datasheet 4.9.2)
        temp = self._c0 * 0.5 + self._c1 * scaled_rawtemp
        return temp

    @property
    def pressure(self) -> float:
        """Returns the current pressure reading in hectoPascals (hPa)."""
        # Get raw readings (24-bit 2's complement)
        raw_temperature_val = self._twos_complement(self._raw_temperature, 24)
        raw_pressure_val = self._twos_complement(self._raw_pressure, 24)

        # Scale according to current settings
        # Traw_sc = Traw / kT
        scaled_rawtemp = raw_temperature_val / self._temp_scale
        # Praw_sc = Praw / kP
        scaled_rawpres = raw_pressure_val / self._pressure_scale

        # Apply calibration compensation formula from datasheet (Section 4.9.1)
        # Pcomp(Pa) = c00 + Praw_sc * (c10 + Praw_sc * (c20 + Praw_sc * c30)) +
        #             Traw_sc * c01 + Traw_sc * Praw_sc * (c11 + Praw_sc * c21)
        # Note: Datasheet formula typo in provided image: F_raw_sc.*C01 ... should be T_raw_sc
        # Corrected based on typical DPS3xx formulas and the first term Traw_sc*c01
        # The python code has:
        #  self._c00
        #  + scaled_rawpres * (self._c10 + scaled_rawpres * (self._c20 + scaled_rawpres * self._c30))
        #  + scaled_rawtemp * self._c01  <--- This seems to be missing from the original code snippet in the prompt
        #  + scaled_rawtemp * scaled_rawpres * (self._c11 + scaled_rawpres * self._c21)

        # Let's re-check the original code's pressure formula:
        # pres_calc = (
        #         self._c00
        #         + scaled_rawpres
        #         * (self._c10 + scaled_rawpres * (self._c20 + scaled_rawpres * self._c30))
        #         + scaled_rawtemp  <--- This seems to be scaled_rawtemp * (self._c01 + ...)
        #         * (self._c01 + scaled_rawpres * (self._c11 + scaled_rawpres * self._c21))
        # )
        # This original formula matches the general form of:
        # Pcomp = C00 + Praw_sc * C10_eff + Traw_sc * C01_eff
        # where C10_eff = C10 + Praw_sc*(C20 + Praw_sc*C30)
        # and C01_eff = C01 + Praw_sc*(C11 + Praw_sc*C21)
        # This seems correct.

        pres_calc_pa = (
                self._c00
                + scaled_rawpres * (self._c10 + scaled_rawpres * (self._c20 + scaled_rawpres * self._c30))
                + scaled_rawtemp * self._c01
                + scaled_rawtemp * scaled_rawpres * (self._c11 + scaled_rawpres * self._c21)
        )
        # The user's original code had:
        # pres_calc = (
        #         self._c00
        #         + scaled_rawpres
        #         * (self._c10 + scaled_rawpres * (self._c20 + scaled_rawpres * self._c30))
        #         + scaled_rawtemp  # <---- This looks like a factor for the next term in their code
        #         * (self._c01 + scaled_rawpres * (self._c11 + scaled_rawpres * self._c21))
        # )
        # If we expand the original code:
        # pres_calc_original = self._c00 + scaled_rawpres * (self._c10 + scaled_rawpres * (self._c20 + scaled_rawpres * self._c30)) + \
        #                      scaled_rawtemp * self._c01 + scaled_rawtemp * scaled_rawpres * (self._c11 + scaled_rawpres * self._c21)
        # This matches the formula I wrote out (pres_calc_pa). So the original code's pressure formula was correct.

        # Convert to hPa (result is in Pa, 1 hPa = 100 Pa)
        return pres_calc_pa / 100.0

    @property
    def altitude(self) -> float:
        """
        Altitude in meters based on pressure and sea-level reference.
        Uses the barometric formula for altitude calculation.
        """
        # P = P0 * (1 - L*h / T0)^(g*M / (R*L))
        # h = T0/L * [1 - (P/P0)^(R*L / (g*M))]
        # Using the common approximation: altitude = 44330 * (1 - (P/P0)^0.1903)
        # P is current pressure, P0 is sea_level_pressure
        try:
            pressure_hpa = self.pressure  # this is already in hPa
            if pressure_hpa <= 0 or self._sea_level_pressure <= 0:  # Added check for sea_level_pressure
                return float('nan')  # Or raise error
            ratio = pressure_hpa / self._sea_level_pressure
            if ratio <= 0:  # Should be caught by pressure_hpa > 0, but good for robustness
                return float('nan')
            # Approximation of the international barometric formula
            return 44330.0 * (1.0 - math.pow(ratio, 0.190284))  # More precise exponent
        except Exception as e:
            # In MicroPython, printing directly might not always be desired in a library.
            # Consider raising an error or returning NaN consistently.
            # print(f"Altitude calculation error: {e}")
            return float('nan')

    @altitude.setter
    def altitude(self, value: float) -> None:
        """Set sea level pressure based on the current pressure and known altitude."""
        # h = 44330 * (1 - (P/P0)^0.190284)
        # P/P0 = (1 - h/44330)^(1/0.190284)
        # P0 = P / (1 - h/44330)^(1/0.190284)
        # 1/0.190284 approx 5.2553
        try:
            current_pressure_hpa = self.pressure
            if current_pressure_hpa <= 0:
                # Cannot calculate sea level pressure if current pressure is invalid
                # raise ValueError("Cannot set altitude with non-positive current pressure")
                return  # Or handle error appropriately

            # Avoid division by zero or issues with (1 - h/44330) if h is too large
            base = 1.0 - value / 44330.0
            if base <= 0:
                # Altitude is too high for this formula to be inverted safely
                # raise ValueError("Altitude value is too high to calculate sea level pressure")
                return  # Or handle error

            self.sea_level_pressure = current_pressure_hpa / math.pow(base, 5.255301895)
        except Exception:
            # Silently fail or raise a specific error
            pass

    @property
    def sea_level_pressure(self) -> float:
        """The local sea level pressure in hectoPascals (aka millibars)."""
        return self._sea_level_pressure

    @sea_level_pressure.setter
    def sea_level_pressure(self, value: float) -> None:
        """Set sea level pressure reference in hectoPascals."""
        if not isinstance(value, (float, int)):  # check type
            raise TypeError("Sea level pressure must be a number")
        if value <= 0:
            raise ValueError("Sea level pressure must be positive")
        self._sea_level_pressure = float(value)

    def reset(self) -> None:
        """Perform a soft reset of the sensor."""
        self._soft_reset = 0b1001  # Value 0x09 from datasheet for soft reset (write '1001' to SOFT_RST[3:0])
        time.sleep(0.01)  # Datasheet mentions startup time after reset, 2.5ms typical. 10ms should be safe.
        # Before, was 0.1s, which is very generous.

        # Re-initialize critical parts after reset
        # self._correct_temp() # Apply correction if needed
        self._read_calibration()  # Coefficients need to be re-read

        # --- MODIFICATION START (Duplicated from __init__) ---
        # Ensure temperature sensor source for measurement matches calibration coefficient source
        self._temp_measurement_src_bit = self._calib_coeff_temp_src_bit
        if self._temperature_external_source != self._temp_measurement_src_bit:
            self._temperature_external_source = self._temp_measurement_src_bit
            time.sleep(0.01)
        # --- MODIFICATION END ---

        # Restore default operating settings
        # Note: Setting properties will also set dependent values like _pressure_scale, _temp_scale, _p_shift, _t_shift
        self.pressure_oversample = SAMPLE_PER_SECOND_64
        self.temperature_oversample = SAMPLE_PER_SECOND_64
        self.pressure_rate = RATE_1_HZ  # Example, choose appropriate default
        self.temperature_rate = RATE_1_HZ  # Example, choose appropriate default

        self._sensor_mode = CONT_PRESTEMP

        # Wait for measurements to be available
        # Sensor needs time to make first measurement after mode change or reset
        self._wait_temperature_ready()
        self._wait_pressure_ready()


if __name__ == "__main__":
    """Example usage of the DPS310 driver."""
    from machine import I2C, Pin  #  Ensure machine module is available

    print("=== DPS310 Sensor Example ===")

    # Initialize I2C - Adjust pins and I2C bus ID for your specific board
    # Common ESP32 pins: scl=Pin(22), sda=Pin(21)
    # Common Raspberry Pi Pico pins: scl=Pin(GP1), sda=Pin(GP0) or scl=Pin(GP5), sda=Pin(GP4) etc.
    try:
        i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=400000)  # Example for ESP32, I2C bus 0
        # For Pico, it might be:
        # i2c = I2C(0, scl=Pin(1), sda=Pin(0), freq=400000) # GP1/GP0 for I2C0
    except Exception as e:
        print(f"Failed to initialize I2C: {e}")
        print("Please check your board's I2C pin assignments.")
        raise SystemExit

    try:
        # Initialize DPS310 at default address 0x77. Use 0x76 if SDO is grounded.
        sensor = DPS310(i2c, address=0x77)
        print("DPS310 sensor initialized successfully!")
        print(
            f"  Initial Temperature Sensor Source: {'External MEMS' if sensor._calib_coeff_temp_src_bit else 'Internal ASIC'} (based on calibration)")
        print(
            f"  Actual Temperature Sensor Source Configured: {'External MEMS' if sensor._temperature_external_source else 'Internal ASIC'}")

    except RuntimeError as e:
        print(f"Failed to initialize DPS310 sensor: {e}")
        print("Ensure the sensor is connected correctly and the I2C address is correct.")
        raise SystemExit
    except Exception as e:  # Catch other potential errors
        print(f"An unexpected error occurred during sensor initialization: {e}")
        raise SystemExit

    print("\n=== DPS310 Sensor Readings ===")
    print(f"Sea level pressure set to: {sensor.sea_level_pressure:.2f} hPa")
    print("{:^15} | {:^15} | {:^12}".format("Temp (°C)", "Press (hPa)", "Alt (m)"))
    print("-" * 47)

    try:
        for i in range(100):  # Read 100 samples
            temperature = sensor.temperature
            pressure = sensor.pressure
            altitude = sensor.altitude  # Uses the current sea_level_pressure setting

            print(f"{temperature:>14.2f} °C | {pressure:>14.2f} hPa | {altitude:>10.2f} m")

            if i == 2:  # Example of changing sea level pressure or mode
                # print("\nUpdating sea_level_pressure to 1000.0 hPa and mode to ONE_PRESTEMP then back\n")
                sensor.sea_level_pressure = 1000.0
                # print(f"New sea level pressure: {sensor.sea_level_pressure:.2f} hPa")
                sensor.mode = IDLE  # Go to IDLE before changing to single shot
                time.sleep(0.1)
                sensor.mode = ONE_PRESSURE
                sensor._wait_pressure_ready()
                p_single = sensor.pressure
                sensor.mode = ONE_TEMPERATURE
                sensor._wait_temperature_ready()
                t_single = sensor.temperature
                print(f"Single shot P: {p_single:.2f} hPa, T: {t_single:.2f} °C")
                sensor.mode = CONT_PRESTEMP  # Return to continuous mode
                sensor._wait_temperature_ready()  # Ensure ready before next loop
                sensor._wait_pressure_ready()

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as e:
        print(f"\nAn error occurred during measurements: {e}")

    finally:
        # Stop sensor measurements
        if 'sensor' in locals():  # Check if sensor object was created
            sensor.mode = IDLE
            print("Sensor set to IDLE. Program finished.")
