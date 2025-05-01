"""
I483 - Kadai1 - XU Pengfei(2510082)
"""
import scd41_example  # Import the example module
from utils import show_mac_address, connect_wifi, sync_rtc


def main():
    """Main function"""
    print("\n=== I483 - Kadai1 - XU Pengfei(2510082) ===")

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
