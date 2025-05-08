#include <endian.h>
#include <stdio.h>
#include <string.h>

#include <inttypes.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "driver/i2c_master.h"
#include "esp_err.h"
#include "esp_flash.h"
#include "esp_log.h"
#include "esp_system.h"

#include "sdkconfig.h"

static char *TAG = "i2c new api";

#define I2C_MASTER_SCL_IO 19
#define I2C_MASTER_SDA_IO 18

#define I2C_ADDR_SCD41 0x62

i2c_master_bus_handle_t bus;
i2c_master_dev_handle_t scd41;

void setup_i2c(void) {
  ESP_LOGI(TAG, "Setting up I2C bus...");
  i2c_master_bus_config_t bus_config = {.clk_source = I2C_CLK_SRC_DEFAULT,
                                        .glitch_ignore_cnt = 7,
                                        .i2c_port = 0,
                                        .scl_io_num = I2C_MASTER_SCL_IO,
                                        .sda_io_num = I2C_MASTER_SDA_IO,
                                        .flags.enable_internal_pullup = true};

  ESP_ERROR_CHECK(i2c_new_master_bus(&bus_config, &bus));

  ESP_LOGI(TAG, "Scanning I2C bus for devices...");
  for (int addr = 1; addr < 127; addr++) {
      if (i2c_master_probe(bus, addr, 1000) == ESP_OK) {
          printf("Found device at 0x%02x\n", addr);
      }
  }

  ESP_LOGI(TAG, "Probing SCD41 device...");
  esp_err_t result = i2c_master_probe(bus, I2C_ADDR_SCD41, 1000);
  if (ESP_OK == result) {
    printf("SCD41 detected!\n");
    ESP_ERROR_CHECK(i2c_master_bus_add_device(
        bus,
        &((const i2c_device_config_t){.device_address = I2C_ADDR_SCD41,
                                      .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                                      .scl_speed_hz = 100000}),
        &scd41));
    ESP_LOGI(TAG, "SCD41 device added successfully");
  } else {
    printf("Failed to detect SCD41!\n");
    esp_restart();
  }
}

static esp_err_t get_serial_code() {
  ESP_LOGI(TAG, "Getting SCD41 serial code...");
  uint8_t write_buf[2] = {0x36, 0x82};
  uint8_t read_buf[9];
  memset(read_buf, 0, sizeof(read_buf));

  esp_err_t res = i2c_master_transmit_receive(
      scd41, write_buf, sizeof(write_buf), read_buf, sizeof(read_buf), 100);

  if (res != ESP_OK) {
    ESP_LOGE(TAG, "Failed to get serial code");
  } else {
    printf("Serial code raw data: ");
    for (size_t i = 0; i < sizeof(read_buf); i++) {
      printf("0x%.2x ", read_buf[i]);
    }
    printf("\n");
  }
  return res;
}

static void stop_periodic_measurements() {
  ESP_LOGI(TAG, "Stopping periodic measurements...");
  uint8_t write_buf[2] = {0x3F, 0x86};

  esp_err_t res = i2c_master_transmit(scd41, write_buf, sizeof(write_buf), 100);
  ESP_ERROR_CHECK(res);
}

static void start_periodic_measurements() {
  ESP_LOGI(TAG, "Starting periodic measurements...");
  uint8_t write_buf[2] = {0x21, 0xB1};

  esp_err_t res = i2c_master_transmit(scd41, write_buf, sizeof(write_buf), 100);
  ESP_ERROR_CHECK(res);
}

void setup_scd41(void) {
  ESP_LOGI(TAG, "Setting up SCD41 sensor...");
  stop_periodic_measurements();
  vTaskDelay(500 / portTICK_PERIOD_MS);
  get_serial_code();
  vTaskDelay(500 / portTICK_PERIOD_MS);
  start_periodic_measurements();
  vTaskDelay(500 / portTICK_PERIOD_MS);
}

static int scd41_is_data_ready() {
  ESP_LOGI(TAG, "Checking if data is ready...");
  uint8_t req[] = {0xe4, 0xb8};
  uint8_t resp[] = {0, 0, 0};
  ESP_ERROR_CHECK(i2c_master_transmit_receive(scd41, req, sizeof(req), resp, sizeof(resp), 100));

  int16_t *raw_data = (int16_t *)&resp;
  uint16_t word = be16toh(*raw_data);
  printf("Data ready response: %u (0x%.2x 0x%.2x)\n", word, resp[0], resp[1]);
  uint16_t mask_check = 0x07ffu;
  return mask_check & word;
}

#define CRC8_POLYNOMIAL 0x31
#define CRC8_INT 0xff
// CRC calculation routine, as found in the scd41 datasheet
uint8_t sensirion_common_generate_crc(const uint8_t *data, uint16_t count) {
  uint16_t current_byte;
  uint8_t crc = CRC8_INT;
  uint8_t crc_bit;
  for (current_byte = 0; current_byte < count; ++current_byte) {
    crc ^= (data[current_byte]);
    for (crc_bit = 8; crc_bit > 0; --crc_bit) {
      if (crc & 0x80)
        crc = (crc << 1) ^ CRC8_POLYNOMIAL;
      else
        crc = (crc << 1);
    }
  }
  return crc;
}

static bool scd41_is_data_crc_correct(uint8_t *raw) {
  for (int i = 0; i < 3; i++) {
    if (sensirion_common_generate_crc(&raw[i * 3], 3)) {
      ESP_LOGE(TAG, "SCD41: CRC ERROR at word number %d\n", i);
      return false;
    }
  }
  return true;
}

static double convert_raw_temperature(uint16_t raw_temp) {
  return -45 + 175 * (raw_temp / (double)0xffff);
}

static double convert_raw_humidity(uint16_t raw_hum) {
  return 100 * (raw_hum / (double)0xffff);
}

static void scd41_get_measurements() {
  ESP_LOGI(TAG, "Reading measurements from SCD41...");
  uint8_t req[] = {0xec, 0x05};
  uint8_t resp[9];
  memset(resp, 0, sizeof(resp));

  ESP_ERROR_CHECK(i2c_master_transmit_receive(scd41, req, sizeof(req), resp, sizeof(resp), 100));

  if (!scd41_is_data_crc_correct(resp)) {
    printf("SCD41: CRC error!\n");
    return;
  }

  int16_t *raw_data = (int16_t *)&resp;
  uint16_t co2 = be16toh(*raw_data);

  raw_data = (int16_t *)&resp[3];
  uint16_t rawtemp = be16toh(*raw_data);

  raw_data = (int16_t *)&resp[6];
  uint16_t rawhumidity = be16toh(*raw_data);

  printf("CO2: %d ppm, Raw Temp: %d, Raw Humidity: %d\n", co2, rawtemp, rawhumidity);
  printf("Temperature: %.2f C, Humidity: %.2f %%\n",
         convert_raw_temperature(rawtemp), convert_raw_humidity(rawhumidity));
}

void poll_scd41(void *pvParameters) {
  ESP_LOGI(TAG, "Starting SCD41 polling task...");
  while (1) {
    vTaskDelay(1000 / portTICK_PERIOD_MS);
    printf(".\n");
    if (scd41_is_data_ready()) {
      ESP_LOGI(TAG, "Data ready, reading...");
      scd41_get_measurements();
    }
  }
}

void app_main(void) {
  ESP_LOGI(TAG, "Application started");
  setup_i2c();
  setup_scd41();
  printf("Hello world!\n");

  xTaskCreate(poll_scd41, "poll_scd41_task", 4096, NULL, 5, NULL);
}
