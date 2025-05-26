"""
MicroPython SCD41 CO2 Sensor Driver
Based on Sensirion SCD4x C Library Implementation
For ESP32 Platform
"""
import time
from machine import I2C, Pin

from sensor import Sensor
from utils import current_time

# SCD41 I2C Address
SCD41_I2C_ADDR_62 = 0x62

# Error Codes
NO_ERROR = 0
ERROR_GENERAL = 1

# Commands
CMD_START_PERIODIC_MEASUREMENT = 0x21B1
CMD_STOP_PERIODIC_MEASUREMENT = 0x3F86
CMD_GET_DATA_READY_STATUS = 0xE4B8
CMD_READ_MEASUREMENT = 0xEC05
CMD_SET_TEMPERATURE_OFFSET = 0x241D
CMD_GET_TEMPERATURE_OFFSET = 0x2318
CMD_SET_SENSOR_ALTITUDE = 0x2427
CMD_GET_SENSOR_ALTITUDE = 0x2322
CMD_SET_AMBIENT_PRESSURE = 0xE000
CMD_PERFORM_FORCED_RECALIBRATION = 0x362F
CMD_GET_AUTOMATIC_SELF_CALIBRATION = 0x2313
CMD_SET_AUTOMATIC_SELF_CALIBRATION = 0x2416
CMD_START_LOW_POWER_PERIODIC_MEASUREMENT = 0x21AC
CMD_GET_SERIAL_NUMBER = 0x3682
CMD_PERFORM_SELF_TEST = 0x3639
CMD_PERFORM_FACTORY_RESET = 0x3632
CMD_REINIT = 0x3646
CMD_MEASURE_SINGLE_SHOT = 0x219D
CMD_MEASURE_SINGLE_SHOT_RHT_ONLY = 0x2196
CMD_POWER_DOWN = 0x36E0
CMD_WAKE_UP = 0x36F6


class SCD41(Sensor):
    """SCD41 CO2 Sensor Driver Class"""

    def __init__(self, i2c: I2C, address=SCD41_I2C_ADDR_62):
        """
        Initialize SCD41 sensor

        Parameters:
            i2c: I2C instance
            address: I2C address of the sensor (default: 0x62)
        """
        super().__init__(i2c, "SCD41", address=address)
        self.i2c = i2c
        self.address = address
        
    def start(self):
        """Initialize the sensor for measurements"""
        self.stop_periodic_measurement()
        self.reinit()
        self.start_periodic_measurement()
        return True
        
    def read(self):
        """Read sensor data and return as dictionary"""
        error, data_ready = self.get_data_ready_status()
        
        if error == NO_ERROR and data_ready:
            error, co2, temperature, humidity = self.read_measurement()
            if error == NO_ERROR:
                self.data = {
                    'co2': co2,
                    'temperature': temperature,
                    'humidity': humidity
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
            return "SCD41 CO2 Sensor: No data available"
            
        result = "SCD41 CO2 Sensor:\n"
        if 'co2' in data:
            result += f"  CO2: {data['co2']} ppm\n"
        if 'temperature' in data:
            result += f"  Temperature: {data['temperature']:.2f} °C\n"
        if 'humidity' in data:
            result += f"  Humidity: {data['humidity']:.2f} %"
            
        return result

    def _calculate_crc(self, data):
        """
        Calculate CRC checksum
        
        Parameters:
            data: Two-byte data
            
        Returns:
            CRC checksum
        """
        crc = 0xFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x80:
                    crc = (crc << 1) ^ 0x31
                else:
                    crc = crc << 1
        return crc & 0xFF

    def _send_command(self, cmd):
        """
        Send command to sensor
        
        Parameters:
            cmd: Command code
            
        Returns:
            NO_ERROR on success, error code on failure
        """
        try:
            cmd_bytes = [(cmd >> 8) & 0xFF, cmd & 0xFF]
            self.i2c.writeto(self.address, bytes(cmd_bytes))
            return NO_ERROR
        except Exception as e:
            print(f"Error sending command: {e}")
            return ERROR_GENERAL

    def _send_command_with_args(self, cmd, data_words):
        """
        Send command with arguments
        
        Parameters:
            cmd: Command code
            data_words: List of parameters, each parameter is a 16-bit integer
            
        Returns:
            NO_ERROR on success, error code on failure
        """
        try:
            buffer = [(cmd >> 8) & 0xFF, cmd & 0xFF]

            for word in data_words:
                msb = (word >> 8) & 0xFF
                lsb = word & 0xFF
                buffer.append(msb)
                buffer.append(lsb)
                buffer.append(self._calculate_crc([msb, lsb]))

            self.i2c.writeto(self.address, bytes(buffer))
            return NO_ERROR
        except Exception as e:
            print(f"Error sending command with arguments: {e}")
            return ERROR_GENERAL

    def _read_words(self, num_words):
        """
        Read data from sensor
        
        Parameters:
            num_words: Number of words to read
            
        Returns:
            (error_code, data_words)
            error_code: Error code
            data_words: Read data, each element is a 16-bit integer
        """
        try:
            # Each word requires 3 bytes (2 data bytes + 1 CRC byte)
            data = self.i2c.readfrom(self.address, num_words * 3)
            words = []

            for i in range(0, len(data), 3):
                msb = data[i]
                lsb = data[i + 1]
                crc = data[i + 2]

                # Verify CRC
                if self._calculate_crc([msb, lsb]) != crc:
                    print(f"CRC error: Calculated {self._calculate_crc([msb, lsb])} != Received {crc}")
                    return ERROR_GENERAL, []

                word = (msb << 8) | lsb
                words.append(word)

            return NO_ERROR, words
        except Exception as e:
            print(f"Error reading data: {e}")
            return ERROR_GENERAL, []

    def _read_command(self, cmd, num_words):
        """
        Send command and read response
        
        Parameters:
            cmd: Command code
            num_words: Number of words to read
            
        Returns:
            (error_code, data_words)
            error_code: Error code
            data_words: Read data, each element is a 16-bit integer
        """
        error = self._send_command(cmd)
        if error:
            return error, []

        return self._read_words(num_words)

    def _delayed_read_command(self, cmd, delay_ms, num_words):
        """
        Send command, delay, then read response
        
        Parameters:
            cmd: Command code
            delay_ms: Delay in milliseconds
            num_words: Number of words to read
            
        Returns:
            (error_code, data_words)
            error_code: Error code
            data_words: Read data, each element is a 16-bit integer
        """
        error = self._send_command(cmd)
        if error:
            return error, []

        time.sleep_ms(delay_ms)

        return self._read_words(num_words)

    def init(self):
        """
        Initialize sensor
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        return NO_ERROR  # ESP32's I2C is already initialized in constructor

    def wake_up(self):
        """
        Wake up sensor
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        error = self._send_command(CMD_WAKE_UP)
        time.sleep_ms(20)  # Wait for sensor to wake up
        return error

    def stop_periodic_measurement(self):
        """
        Stop periodic measurement
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        error = self._send_command(CMD_STOP_PERIODIC_MEASUREMENT)
        time.sleep_ms(500)  # Wait for sensor to stop measurement
        return error

    def reinit(self):
        """
        Reinitialize sensor
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        error = self._send_command(CMD_REINIT)
        time.sleep_ms(20)  # Wait for sensor to reinitialize
        return error

    def get_serial_number(self):
        """
        Get sensor serial number
        
        Returns:
            (error_code, serial_number)
            error_code: Error code
            serial_number: Serial number (integer)
        """
        error, words = self._read_command(CMD_GET_SERIAL_NUMBER, 3)
        if error:
            return error, 0

        # Combine 3 16-bit words into a single serial number
        serial = (words[0] << 32) | (words[1] << 16) | words[2]
        return NO_ERROR, serial

    def start_periodic_measurement(self):
        """
        Start periodic measurement (5 second interval)
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        return self._send_command(CMD_START_PERIODIC_MEASUREMENT)

    def start_low_power_periodic_measurement(self):
        """
        Start low power periodic measurement (30 second interval)
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        return self._send_command(CMD_START_LOW_POWER_PERIODIC_MEASUREMENT)

    def get_data_ready_status(self):
        """
        Check if data is ready
        
        Returns:
            (error_code, data_ready)
            error_code: Error code
            data_ready: Whether data is ready (boolean)
        """
        error, words = self._read_command(CMD_GET_DATA_READY_STATUS, 1)
        if error:
            return error, False

        # Check data ready bit
        data_ready = (words[0] & 0x07FF) != 0
        return NO_ERROR, data_ready

    def read_measurement(self):
        """
        Read measurement data
        
        Returns:
            (error_code, co2, temperature, humidity)
            error_code: Error code
            co2: CO2 concentration (ppm)
            temperature: Temperature (°C)
            humidity: Relative humidity (%)
        """
        error, words = self._read_command(CMD_READ_MEASUREMENT, 3)
        if error:
            return error, 0, 0, 0

        co2 = words[0]

        # Integer division optimization 65535 -> 65536
        # Convert temperature: Raw value is 175 * 2^16 * T / 65536 - 45
        # Simplified to: 175 * T / 65536 - 45
        temp_raw = words[1]
        temperature = 175.0 * temp_raw / 65536.0 - 45.0

        # Convert humidity: Raw value is 100 * 2^16 * RH / 65536
        # Simplified to: 100 * RH / 65536
        hum_raw = words[2]
        humidity = 100.0 * hum_raw / 65536.0

        return NO_ERROR, co2, temperature, humidity

    def measure_single_shot(self):
        """
        Perform single shot measurement
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        error = self._send_command(CMD_MEASURE_SINGLE_SHOT)
        time.sleep_ms(5000)  # Wait for measurement to complete
        return error

    def measure_single_shot_rht_only(self):
        """
        Perform single shot temperature and humidity measurement (no CO2)
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        error = self._send_command(CMD_MEASURE_SINGLE_SHOT_RHT_ONLY)
        time.sleep_ms(50)  # Wait for measurement to complete
        return error

    def set_temperature_offset(self, offset_celsius):
        """
        Set temperature offset
        
        Parameters:
            offset_celsius: Temperature offset (°C)
            
        Returns:
            NO_ERROR on success, error code on failure
        """
        # Convert to sensor format: T_offset * 2^16 / 175
        offset_ticks = int((offset_celsius * 65536.0) / 175.0)
        return self._send_command_with_args(CMD_SET_TEMPERATURE_OFFSET, [offset_ticks])

    def get_temperature_offset(self):
        """
        Get temperature offset
        
        Returns:
            (error_code, offset_celsius)
            error_code: Error code
            offset_celsius: Temperature offset (°C)
        """
        error, words = self._read_command(CMD_GET_TEMPERATURE_OFFSET, 1)
        if error:
            return error, 0

        # Convert to Celsius: T_offset * 175 / 2^16
        offset_ticks = words[0]
        offset_celsius = (offset_ticks * 175.0) / 65536.0

        return NO_ERROR, offset_celsius

    def set_sensor_altitude(self, altitude_meters):
        """
        Set sensor altitude
        
        Parameters:
            altitude_meters: Altitude (meters)
            
        Returns:
            NO_ERROR on success, error code on failure
        """
        return self._send_command_with_args(CMD_SET_SENSOR_ALTITUDE, [altitude_meters])

    def get_sensor_altitude(self):
        """
        Get sensor altitude
        
        Returns:
            (error_code, altitude_meters)
            error_code: Error code
            altitude_meters: Altitude (meters)
        """
        error, words = self._read_command(CMD_GET_SENSOR_ALTITUDE, 1)
        if error:
            return error, 0

        return NO_ERROR, words[0]

    def set_ambient_pressure(self, pressure_pa):
        """
        Set ambient pressure
        
        Parameters:
            pressure_pa: Ambient pressure (Pa)
            
        Returns:
            NO_ERROR on success, error code on failure
        """
        # Convert to sensor format: Pressure(Pa) / 100
        pressure_hpa = int(pressure_pa / 100)
        return self._send_command_with_args(CMD_SET_AMBIENT_PRESSURE, [pressure_hpa])

    def perform_forced_recalibration(self, target_co2_ppm):
        """
        Perform forced recalibration
        
        Parameters:
            target_co2_ppm: Target CO2 concentration (ppm)
            
        Returns:
            (error_code, frc_correction)
            error_code: Error code
            frc_correction: FRC correction value
        """
        error = self._send_command_with_args(CMD_PERFORM_FORCED_RECALIBRATION, [target_co2_ppm])
        if error:
            return error, 0

        time.sleep_ms(400)  # Wait for calibration to complete

        error, words = self._read_words(1)
        if error:
            return error, 0

        # Interpret FRC correction value
        if words[0] == 0xFFFF:
            return ERROR_GENERAL, 0  # Calibration failed

        return NO_ERROR, words[0]

    def set_automatic_self_calibration(self, enable):
        """
        Set automatic self calibration
        
        Parameters:
            enable: Whether to enable automatic self calibration (boolean)
            
        Returns:
            NO_ERROR on success, error code on failure
        """
        return self._send_command_with_args(CMD_SET_AUTOMATIC_SELF_CALIBRATION, [1 if enable else 0])

    def get_automatic_self_calibration(self):
        """
        Get automatic self calibration status
        
        Returns:
            (error_code, enabled)
            error_code: Error code
            enabled: Whether automatic self calibration is enabled (boolean)
        """
        error, words = self._read_command(CMD_GET_AUTOMATIC_SELF_CALIBRATION, 1)
        if error:
            return error, False

        return NO_ERROR, words[0] == 1

    def perform_self_test(self):
        """
        Perform self test
        
        Returns:
            (error_code, self_test_result)
            error_code: Error code
            self_test_result: Self test result (0 means success)
        """
        error, words = self._delayed_read_command(CMD_PERFORM_SELF_TEST, 10000, 1)
        if error:
            return error, 0

        return NO_ERROR, words[0]

    def perform_factory_reset(self):
        """
        Perform factory reset
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        error = self._send_command(CMD_PERFORM_FACTORY_RESET)
        time.sleep_ms(1200)  # Wait for reset to complete
        return error

    def power_down(self):
        """
        Power down sensor
        
        Returns:
            NO_ERROR on success, error code on failure
        """
        return self._send_command(CMD_POWER_DOWN)


class SensorData:
    """Class to store and format sensor data"""

    def __init__(self, co2=0, temperature=0.0, humidity=0.0, timestamp=None):
        self.co2 = co2
        self.temperature = temperature
        self.humidity = humidity
        self.timestamp = timestamp or time.time()

    def __str__(self):
        """Format sensor data as a string"""
        return (f"{current_time()} [{self._format_time()}]:\n"
                f"  CO2: {self.co2} ppm\n"
                f"  Temp: {self.temperature:.2f} °C\n"
                f"  RH: {self.humidity:.2f} %")

    def _format_time(self):
        """Format timestamp into readable format"""
        # Simple format - seconds since boot
        return f"T+{self.timestamp:.1f}s"

    def as_dict(self):
        """Return data as dictionary for transmission"""
        return {
            "co2": self.co2,
            "temperature": self.temperature,
            "humidity": self.humidity,
            "timestamp": self.timestamp
        }


def main():
    # Initialize I2C
    # Default I2C pins on ESP32: SCL=22, SDA=21
    print("\n=== SCD41 CO2 Sensor Initialization ===")
    i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=100000)

    # Initialize SCD41 sensor
    scd41 = SCD41(i2c)

    # Ensure sensor is in clean state
    print("Initializing sensor...")

    error = scd41.stop_periodic_measurement()
    if error != NO_ERROR:
        print(f"Error stopping periodic measurement: {error}")
        # reinitialize sensor
        error = scd41.reinit()
        if error != NO_ERROR:
            print(f"Error reinitializing: {error}")
        else:
            print("[OK] Sensor reinitialized")
    else:
        print("[OK] Periodic measurement stopped")

    # Read sensor information
    error, serial = scd41.get_serial_number()
    if error != NO_ERROR:
        print(f"Error getting serial number: {error}")
        return

    print(f"[OK] Sensor detected - Serial: 0x{serial:X}")

    # Start periodic measurement (5 second interval)
    error = scd41.start_periodic_measurement()
    if error != NO_ERROR:
        print(f"Error starting periodic measurement: {error}")
        return

    print("[OK] Periodic measurement started")
    print("\n=== SCD41 CO2 Sensor Readings ===")
    print("Reading data every 5 seconds...\n")

    # Store readings for potential transmission
    readings = []

    # Read data 50 times
    start_time = time.time()
    for i in range(50):
        # Wait 5 seconds
        time.sleep(5)

        # Check if data is ready
        error, data_ready = scd41.get_data_ready_status()
        if error != NO_ERROR:
            print(f"Error getting data ready status: {error}")
            continue

        # Wait for data to be ready
        while not data_ready:
            time.sleep_ms(100)
            error, data_ready = scd41.get_data_ready_status()
            if error != NO_ERROR:
                print(f"Error getting data ready status: {error}")
                break

        if not data_ready:
            continue

        # Read measurement data
        error, co2, temperature, humidity = scd41.read_measurement()
        if error != NO_ERROR:
            print(f"Error reading measurement data: {error}")
            continue

        # Create and store reading
        current_time = time.time() - start_time
        reading = SensorData(co2, temperature, humidity, current_time)
        readings.append(reading)

        # Print formatted results
        print(reading)
        print("-" * 40)

    # Stop measurement
    scd41.stop_periodic_measurement()
    print("\n=== Measurement Complete ===")
    print(f"Total readings: {len(readings)}")


if __name__ == "__main__":
    main()
