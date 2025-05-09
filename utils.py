import network
import ntptime
import time
from machine import RTC

# WiFi configuration
# List of WiFi networks to try in order [(ssid1, password1), (ssid2, password2), ...]
WIFI_NETWORKS = [
    ("佑希柯のスマホ", "xuing233"),  # Primary network - ASCII name to avoid encoding issues
    ("JAISTALL", ""),  # Backup network 1
    # ("eduroam", "edu_password"),      # Backup network 2
    # Add more networks as needed
]

# Maximum number of connection attempts per WiFi network
MAX_CONNECTION_ATTEMPTS = 3  # Try each network this many times before moving to next one

# NTP server configuration
NTP_SERVER = "ntp.nict.jp"  # Japan standard time server, can be changed as needed

rtc = RTC()

def show_mac_address():
    """Show MAC address"""
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    mac = wlan.config('mac')
    mac_str = ':'.join(['%02X' % b for b in mac])
    print("MAC address:", mac_str)


def connect_wifi():
    """Connect to Wi-Fi network using the predefined WIFI_NETWORKS list
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    print("Connecting to WiFi...")
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    
    # If already connected, return True
    if wlan.isconnected():
        print("Already connected to WiFi")
        print(f"IP address: {wlan.ifconfig()[0]}")
        return True
    
    # Try to connect to each network in the list
    for network_ssid, network_password in WIFI_NETWORKS:
        # Try connecting to this network multiple times
        for attempt in range(1, MAX_CONNECTION_ATTEMPTS + 1):
            try:
                print(f"Attempting to connect to network: {network_ssid} (Attempt {attempt}/{MAX_CONNECTION_ATTEMPTS})")
                wlan.connect(network_ssid, network_password)
                
                # Wait for connection or timeout
                max_wait = 20
                while max_wait > 0:
                    if wlan.isconnected():
                        print("WiFi connection successful")
                        print(f"IP address: {wlan.ifconfig()[0]}")
                        return True
                    max_wait -= 1
                    print("Waiting for connection...")
                    time.sleep(1)
                
                print(f"Failed to connect to {network_ssid} on attempt {attempt}/{MAX_CONNECTION_ATTEMPTS}")
                # If we've reached the maximum number of attempts, move to the next network
                if attempt < MAX_CONNECTION_ATTEMPTS:
                    print("Retrying...")
                    time.sleep(2)  # Wait before retrying
                else:
                    print("Maximum connection attempts reached, trying next network if available...")
            except OSError as e:
                print(f"WiFi connection error for {network_ssid}: {e} (Attempt {attempt}/{MAX_CONNECTION_ATTEMPTS})")
                # If we've reached the maximum number of attempts, move to the next network
                if attempt < MAX_CONNECTION_ATTEMPTS:
                    print("Retrying after error...")
                    time.sleep(2)  # Wait before retrying
                else:
                    print("Maximum connection attempts reached, trying next network if available...")
                    time.sleep(2)  # Give some time before trying the next network
    
    print("WiFi connection failed for all networks")
    return False


def sync_rtc():
    """Sync RTC time to Japan timezone (UTC+9)"""
    print("Syncing RTC time...")

    try:
        # Set NTP server
        ntptime.host = NTP_SERVER
        ntptime.settime()

        # Add timezone offset
        ts = time.time() + 9 * 3600
        t = time.gmtime(ts)  # Change to gmtime to ensure t is JST time structure

        rtc.datetime((
            t[0], t[1], t[2],  # year, month, day
            t[6],              # weekday
            t[3], t[4], t[5],  # hour, min, sec
            0                  # subseconds
        ))

        print(current_time())
        return True
    except Exception as e:
        print(f"RTC time sync failed: {e}")
        return False


def current_time():
    """Get current time (already in Japan timezone)"""
    t = rtc.datetime()
    year, month, day, weekday, hours, minutes, seconds, _ = t
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(year, month, day, hours, minutes, seconds)


def print_time():
    """Print current time"""
    print(f"Current time: {current_time()}")

