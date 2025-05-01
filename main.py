"""
SCD41 CO2 Sensor Main Program
- Connect to WiFi
- Sync RTC time
- Run SCD41 sensor example
"""
import scd41_example  # Import the example module
from utils import show_mac_address, connect_wifi, sync_rtc


def main():
    """Main function"""
    print("\n=== SCD41 CO2 Sensor with WiFi/RTC Integration ===")

    # Show MAC address
    show_mac_address()

    # Connect to Wi-Fi
    if connect_wifi():
        # Sync RTC time
        sync_rtc()
    else:
        print("Cannot sync RTC time because WiFi connection failed")

    # Run SCD41 example directly
    print("\nStarting SCD41 sensor example...")
    scd41_example.main()


if __name__ == "__main__":
    main()
