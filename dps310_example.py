#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DPS310 Pressure and Temperature Sensor Example
For ESP32 platform with MicroPython environment
"""

import time
from machine import I2C, Pin
from dps310 import DPS310, Rate, Oversample, Mode, NO_ERROR

def main():
    # Create I2C object
    # Default I2C pins on ESP32
    # SCL: GPIO 22
    # SDA: GPIO 21
    # Adjust pins according to your ESP32 wiring
    i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=100000)
    
    print("\n=== DPS310 Pressure and Temperature Sensor Initialization ===")
    
    # Scan I2C bus
    devices = i2c.scan()
    if devices:
        print(f"Found {len(devices)} devices on I2C bus:")
        for device in devices:
            print(f"  Device address: 0x{device:02X}")
    else:
        print("No devices found on I2C bus, please check connections")
        return
    
    # Initialize DPS310 sensor
    # Note: Default I2C address is 0x77
    # If your device uses a different address, modify the parameter accordingly
    sensor = DPS310(i2c, addr=0x77)
    
    print("Initializing DPS310 sensor...")
    if not sensor.begin():
        print("Failed to initialize DPS310 sensor! Please check connection and address.")
        return
    
    print("DPS310 sensor initialized successfully!")
    
    # Configure sensor
    # Here we use high precision configuration
    sensor.configure_temperature(Rate.HZ_64, Oversample.SAMPLE_64)
    sensor.configure_pressure(Rate.HZ_64, Oversample.SAMPLE_64)
    
    # Set to continuous measurement mode
    sensor.set_mode(Mode.CONT_PRESTEMP)
    
    # Read and display data
    try:
        print("\n=== DPS310 Sensor Readings ===")
        print("{:^10} | {:^10} | {:^10}".format("Temp(°C)", "Press(hPa)", "Alt(m)"))
        print("-" * 36)
        
        count = 0
        while count < 10:  # Read data 10 times
            # Wait for data to be ready
            if sensor.temperature_available() and sensor.pressure_available():
                # Read temperature
                temperature = sensor.read_temperature()
                
                # Read pressure
                pressure = sensor.read_pressure()
                
                # Calculate altitude
                altitude = sensor.read_altitude()
                
                # Display data
                print("{:10.2f} | {:10.2f} | {:10.2f}".format(
                    temperature, pressure/100, altitude))
                
                count += 1
                
            # Wait one second
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nProgram exited")
    
    # Set to idle mode
    sensor.set_mode(Mode.IDLE)
    print("\nMeasurement completed")

if __name__ == "__main__":
    main()