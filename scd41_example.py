"""
SCD41 CO2 Sensor Usage Example
For ESP32 Platform
"""
import time

from machine import I2C, Pin

from scd41 import SCD41, NO_ERROR
from utils import current_time


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