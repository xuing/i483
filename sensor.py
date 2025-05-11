"""I483 - Kadai1 - XU Pengfei(2510082)
Simplified Sensor Base Class"""
import asyncio


class Sensor:
    """Sensor Abstract Base Class
    
    Defines common interfaces for all sensors
    """
    
    def __init__(self, i2c, name, address=None):
        """Initialize sensor
        
        Parameters:
            i2c: I2C object
            name: Sensor name
            addr: Sensor I2C address
        """
        self.i2c = i2c
        self.name = name
        self.address = address
        self.last_read_time = 0
        self.data = {}

    def start(self):
        raise NotImplementedError("Subclasses must implement read_data()")

    def stop(self):
        raise NotImplementedError("Subclasses must implement read_data()")

    def initialize(self):
        raise NotImplementedError("Subclasses must implement initialize()")

    def read_data(self):
        raise NotImplementedError("Subclasses must implement read_data()")

    def get_data(self):
        return self.data

    def available_keys(self):
        return list(self.data.keys())
