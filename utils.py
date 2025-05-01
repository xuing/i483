import network
import ntptime
import time
from machine import RTC

# WiFi configuration
WIFI_SSID = "佑希柯のスマホ"  # Please change to your WiFi SSID
WIFI_PASSWORD = "xuing233"  # Please change to your WiFi password

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


def connect_wifi(ssid=WIFI_SSID, password=WIFI_PASSWORD):
    """Connect to Wi-Fi network"""
    print("Connecting to WiFi...")
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)

    if not wlan.isconnected():
        print(f"Connecting to network: {ssid}")
        wlan.connect(ssid, password)

        # Wait for connection or timeout
        max_wait = 20
        while max_wait > 0:
            if wlan.isconnected():
                break
            max_wait -= 1
            print("Waiting for connection...")
            time.sleep(1)

    if wlan.isconnected():
        print("WiFi connection successful")
        print(f"IP address: {wlan.ifconfig()[0]}")
        return True
    else:
        print("WiFi connection failed")
        return False


def sync_rtc():
    """Sync RTC time to Japan timezone (UTC+9)"""
    print("Syncing RTC time...")

    try:
        # Set NTP server
        ntptime.host = NTP_SERVER
        ntptime.settime()

        # 加上时区偏移
        ts = time.time() + 9 * 3600
        t = time.gmtime(ts)  # 改成 gmtime，确保 t 是 JST 时间结构

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

