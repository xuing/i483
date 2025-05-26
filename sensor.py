"""I483 - Kadai1 - XU Pengfei(2510082)
Simplified Sensor Base Class"""


class Sensor:
    """Sensor Abstract Base Class
    
    Defines common interfaces for all sensors
    """

    def __init__(self, i2c, name, address=None):
        self.i2c = i2c
        self.name = name
        self.address = address
        self.data = {}

    def start(self):
        raise NotImplementedError("Subclasses must implement start()")

    def stop(self):
        raise NotImplementedError("Subclasses must implement stop()")

    def read(self):
        print(f"[ERROR] {self.name} read() method not implemented")
        raise NotImplementedError("Subclasses must implement read()")
