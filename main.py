"""I483 - Kadai1 - XU Pengfei(2510082)
Multi-sensor Data Acquisition System
"""
import time
from machine import I2C, Pin

# Import utility modules
from utils import show_mac_address, connect_wifi, sync_rtc, current_time

# Import sensor modules
from scd41 import SCD41, NO_ERROR
from bh1750 import BH1750, CONT_H_RES_MODE
from rpr0521rs import RPR0521RS
from dps310 import DPS310


class SensorManager:
    """Sensor Manager - Unified management of all sensors"""
    
    def __init__(self):
        # Initialize I2C bus
        self.i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=100000)
        
        # Sensor instance dictionary
        self.sensors = {}
        self.sensor_data = {}
        
        # Initialize all sensors
        self._init_sensors()
    
    def _init_sensors(self):
        """Initialize all sensors"""
        print("\n=== Initializing Sensors ===")
        
        # Initialize SCD41 CO2 sensor
        try:
            scd41 = SCD41(self.i2c)
            # Stop any previous measurements
            scd41.stop_periodic_measurement()
            # Reinitialize
            scd41.reinit()
            # Start periodic measurement
            scd41.start_periodic_measurement()
            self.sensors['scd41'] = scd41
            print("[OK] SCD41 CO2 sensor initialized successfully")
        except Exception as e:
            print(f"[ERROR] SCD41 initialization failed: {e}")
        
        # Initialize BH1750 light sensor
        try:
            bh1750 = BH1750(self.i2c, mode=CONT_H_RES_MODE)
            bh1750.power(True)
            bh1750.reset()
            bh1750.set_mode()
            self.sensors['bh1750'] = bh1750
            print("[OK] BH1750 light sensor initialized successfully")
        except Exception as e:
            print(f"[ERROR] BH1750 initialization failed: {e}")
        
        # Initialize RPR0521RS ambient light/proximity sensor
        try:
            rpr0521rs = RPR0521RS(self.i2c)
            self.sensors['rpr0521rs'] = rpr0521rs
            print("[OK] RPR0521RS ambient light/proximity sensor initialized successfully")
        except Exception as e:
            print(f"[ERROR] RPR0521RS initialization failed: {e}")
        
        # Initialize DPS310 pressure/temperature sensor
        try:
            dps310 = DPS310(self.i2c)
            self.sensors['dps310'] = dps310
            print("[OK] DPS310 pressure/temperature sensor initialized successfully")
        except Exception as e:
            print(f"[ERROR] DPS310 initialization failed: {e}")
    
    def read_all_sensors(self):
        """Read data from all sensors"""
        timestamp = time.time()
        
        # Read SCD41 data
        if 'scd41' in self.sensors:
            scd41 = self.sensors['scd41']
            error, data_ready = scd41.get_data_ready_status()
            
            if error == NO_ERROR and data_ready:
                error, co2, temperature, humidity = scd41.read_measurement()
                if error == NO_ERROR:
                    self.sensor_data['scd41'] = {
                        'co2': co2,
                        'temperature': temperature,
                        'humidity': humidity,
                        'timestamp': timestamp
                    }
        
        # Read BH1750 data
        if 'bh1750' in self.sensors:
            bh1750 = self.sensors['bh1750']
            lux = bh1750.read_light()
            if lux >= 0:
                self.sensor_data['bh1750'] = {
                    'illuminance': lux,
                    'timestamp': timestamp
                }
        
        # Read RPR0521RS data
        if 'rpr0521rs' in self.sensors:
            rpr0521rs = self.sensors['rpr0521rs']
            try:
                ambient_light = rpr0521rs.ambient_light
                proximity = rpr0521rs.proximity
                self.sensor_data['rpr0521rs'] = {
                    'ambient_light': ambient_light,
                    'proximity': proximity,
                    'illumination': rpr0521rs.illumination,
                    'infrared_illumination': rpr0521rs.infrared_illumination,
                    'timestamp': timestamp
                }
            except Exception as e:
                print(f"[ERROR] Failed to read RPR0521RS data: {e}")
        
        # Read DPS310 data
        if 'dps310' in self.sensors:
            dps310 = self.sensors['dps310']
            try:
                temperature = dps310.temperature
                pressure = dps310.pressure
                altitude = dps310.altitude
                self.sensor_data['dps310'] = {
                    'temperature': temperature,
                    'pressure': pressure,
                    'altitude': altitude,
                    'timestamp': timestamp
                }
            except Exception as e:
                print(f"[ERROR] Failed to read DPS310 data: {e}")
        
        return self.sensor_data
    
    def display_sensor_data(self):
        """Display all sensor data"""
        print(f"\n=== Sensor Data [{current_time()}] ===")
        
        # Display SCD41 data
        if 'scd41' in self.sensor_data:
            data = self.sensor_data['scd41']
            print("SCD41 CO2 Sensor:")
            print(f"  CO2: {data['co2']} ppm")
            print(f"  Temperature: {data['temperature']:.2f} °C")
            print(f"  Humidity: {data['humidity']:.2f} %")
        
        # Display BH1750 data
        if 'bh1750' in self.sensor_data:
            data = self.sensor_data['bh1750']
            print("BH1750 Light Sensor:")
            print(f"  Illuminance: {data['illuminance']:.2f} lx")
        
        # Display RPR0521RS data
        if 'rpr0521rs' in self.sensor_data:
            data = self.sensor_data['rpr0521rs']
            print("RPR0521RS Ambient Light/Proximity Sensor:")
            print(f"  Ambient Light: {data['ambient_light']:.2f} lx")
            print(f"  Proximity: {data['proximity']}")
            print(f"  Illumination: {data['illumination']:.2f} lx")
            print(f"  Infrared Illumination: {data['infrared_illumination']:.2f} lx")
        
        # Display DPS310 data
        if 'dps310' in self.sensor_data:
            data = self.sensor_data['dps310']
            print("DPS310 Pressure/Temperature Sensor:")
            print(f"  Temperature: {data['temperature']:.2f} °C")
            print(f"  Pressure: {data['pressure']:.2f} hPa")
            print(f"  Altitude: {data['altitude']:.2f} m")


def main():
    """Main function"""
    print("\n=== I483 - Kadai1 - XU Pengfei(2510082) ===")
    print("Multi-sensor Data Acquisition System")

    show_mac_address()

    if connect_wifi():
        sync_rtc()
    else:
        print("WiFi connection failed, unable to synchronize RTC time")

    sensor_manager = SensorManager()
    
    print("\nStarting to read sensor data...")
    print("Press Ctrl+C to stop the program")
    
    try:
        while True:
            # Read all sensor data
            sensor_manager.read_all_sensors()
            
            # Display sensor data
            sensor_manager.display_sensor_data()
            
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nProgram stopped")
    finally:
        print("Cleaning up resources...")


if __name__ == "__main__":
    main()
