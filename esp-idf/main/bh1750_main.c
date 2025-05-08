#include <stdio.h>
#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "driver/i2c_master.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_system.h"

#include "sdkconfig.h"

static const char *TAG = "kadai1-1-bh1750";

#define I2C_MASTER_SCL_IO 22
#define I2C_MASTER_SDA_IO 21

#define I2C_ADDR_BH1750 0x23  // Default address, can also be 0x5C

// BH1750 command definitions
#define BH1750_PWR_DOWN    0x00
#define BH1750_PWR_ON      0x01
#define BH1750_RESET       0x07

// Measurement modes
#define BH1750_CONT_H_RES_MODE    0x10  // Continuous high resolution mode (1 lx)
#define BH1750_CONT_H_RES_MODE2   0x11  // Continuous high resolution mode 2 (0.5 lx)
#define BH1750_CONT_L_RES_MODE    0x13  // Continuous low resolution mode (4 lx)
#define BH1750_ONE_TIME_H_RES_MODE    0x20  // One-time high resolution mode (1 lx)
#define BH1750_ONE_TIME_H_RES_MODE2   0x21  // One-time high resolution mode 2 (0.5 lx)
#define BH1750_ONE_TIME_L_RES_MODE    0x23  // One-time low resolution mode (4 lx)

i2c_master_bus_handle_t bus;
i2c_master_dev_handle_t bh1750;

// Measurement accuracy factor, default is 1.0
float measurement_accuracy = 1.0;

void setup_i2c(void) {
    ESP_LOGI(TAG, "Setting up I2C bus...");
    i2c_master_bus_config_t bus_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .glitch_ignore_cnt = 7,
        .i2c_port = 0,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .flags.enable_internal_pullup = true
    };

    ESP_ERROR_CHECK(i2c_new_master_bus(&bus_config, &bus));

    ESP_LOGI(TAG, "Scanning I2C bus for devices...");
    for (int addr = 1; addr < 127; addr++) {
        if (i2c_master_probe(bus, addr, 1000) == ESP_OK) {
            printf("Found device at address 0x%02x\n", addr);
        }
    }

    ESP_LOGI(TAG, "Probing BH1750 device...");
    esp_err_t result = i2c_master_probe(bus, I2C_ADDR_BH1750, 1000);
    if (ESP_OK == result) {
        printf("BH1750 detected!\n");
        ESP_ERROR_CHECK(i2c_master_bus_add_device(
            bus,
            &((const i2c_device_config_t){
                .device_address = I2C_ADDR_BH1750,
                .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                .scl_speed_hz = 100000
            }),
            &bh1750));
        ESP_LOGI(TAG, "BH1750 device added successfully");
    } else {
        printf("Failed to detect BH1750!\n");
        esp_restart();
    }
}

static esp_err_t bh1750_send_command(uint8_t cmd) {
    ESP_LOGI(TAG, "Sending command: 0x%02x", cmd);
    uint8_t write_buf[1] = {cmd};
    return i2c_master_transmit(bh1750, write_buf, sizeof(write_buf), 100);
}

static void bh1750_power(bool on) {
    ESP_LOGI(TAG, "%s sensor power", on ? "Turning on" : "Turning off");
    uint8_t cmd = on ? BH1750_PWR_ON : BH1750_PWR_DOWN;
    esp_err_t res = bh1750_send_command(cmd);
    ESP_ERROR_CHECK(res);
    vTaskDelay(10 / portTICK_PERIOD_MS);  // Wait 10ms
}

static void bh1750_reset(void) {
    ESP_LOGI(TAG, "Resetting sensor");
    esp_err_t res = bh1750_send_command(BH1750_RESET);
    ESP_ERROR_CHECK(res);
    vTaskDelay(10 / portTICK_PERIOD_MS);  // Wait 10ms
}

static void bh1750_set_mode(uint8_t mode) {
    ESP_LOGI(TAG, "Setting measurement mode: 0x%02x", mode);
    esp_err_t res = bh1750_send_command(mode);
    ESP_ERROR_CHECK(res);
    
    // Set appropriate wait time based on mode
    if (mode == BH1750_CONT_H_RES_MODE || mode == BH1750_CONT_H_RES_MODE2 ||
        mode == BH1750_ONE_TIME_H_RES_MODE || mode == BH1750_ONE_TIME_H_RES_MODE2) {
        vTaskDelay(150 / portTICK_PERIOD_MS);  // High resolution mode wait 150ms
    } else {
        vTaskDelay(20 / portTICK_PERIOD_MS);   // Low resolution mode wait 20ms
    }
}

static float bh1750_read_light(void) {
    // ESP_LOGI(TAG, "Reading light intensity...");
    uint8_t read_buf[2];
    memset(read_buf, 0, sizeof(read_buf));

    esp_err_t res = i2c_master_receive(bh1750, read_buf, sizeof(read_buf), 100);
    if (res != ESP_OK) {
        ESP_LOGE(TAG, "Failed to read data");
        return -1.0;
    }

    uint16_t raw_val = (read_buf[0] << 8) | read_buf[1];
    float lux = raw_val / 1.2 / measurement_accuracy;
    
    // ESP_LOGI(TAG, "Raw data: 0x%04x, Light intensity: %.2f lx", raw_val, lux);
    return lux;
}

void setup_bh1750(void) {
    ESP_LOGI(TAG, "Setting up BH1750 sensor...");
    bh1750_power(true);  // Turn on power
    bh1750_reset();      // Reset sensor
    bh1750_set_mode(BH1750_CONT_H_RES_MODE);  // Set to continuous high resolution mode
}

void poll_bh1750(void *pvParameters) {
    ESP_LOGI(TAG, "Starting BH1750 polling task...");
    
    TickType_t start_time = xTaskGetTickCount();
    
    while (1) {
        vTaskDelay(1000 / portTICK_PERIOD_MS);  // Read once per second
        
        TickType_t current_time = xTaskGetTickCount();
        uint32_t elapsed_seconds = (current_time - start_time) / configTICK_RATE_HZ;
        
        float lux = bh1750_read_light();
        if (lux >= 0) {
            printf("T+%u | Light intensity: %.2f lx\n", (unsigned int)elapsed_seconds, lux);
        } else {
            printf("T+%u | Failed to read sensor data\n", (unsigned int)elapsed_seconds);
        }
    }
}

void app_main(void) {
    ESP_LOGI(TAG, "Application started");
    ESP_LOGI(TAG, "=== I483 - Kadai1-1 - XU Pengfei(2510082) ===");
    setup_i2c();
    setup_bh1750();
    printf("BH1750 sensor initialization complete!\n");

    xTaskCreate(poll_bh1750, "poll_bh1750_task", 4096, NULL, 5, NULL);
}