"""
I483 - Kadai1 - XU Pengfei(2510082)
"""
import sys

def clear_user_modules(keep=None):
    """
    Delete all user-loaded (non-core) modules from sys.modules.
    This allows force-reloading on next import.
    """
    if keep is None:
        keep = {'sys', 'gc', 'uos', 'machine', 'time', 'network', 'ntptime'}

    for name in list(sys.modules):
        print("Deleting module:", name)
        if name not in keep and not name.startswith('u') and not name.startswith('micropython'):
            sys.modules.pop(name)


clear_user_modules()


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
