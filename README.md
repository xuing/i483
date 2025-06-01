# I483 Course Project - IoT Sensor Data Processing Pipeline

## Overview

This repository contains the complete implementation for the I483 course assignments, covering IoT sensor data acquisition, MQTT/Kafka messaging, and stream analytics using Apache Flink. The project demonstrates a comprehensive IoT data processing pipeline from sensor data collection to real-time analytics and visualization.

## Project Structure

```
├── kadai1_esp-idf/          # Assignment 1: ESP-IDF sensor implementation
├── kadai1_micropython/      # Assignment 1: MicroPython multi-sensor system
├── kadai2_golang_processor/ # Assignment 2: MQTT/Kafka data processing (Go)
├── kadai3_flink_kotlin/     # Assignment 3: Stream analytics (Flink + Kotlin)
├── other_messagepump_kotlin_refactoring/ # Additional message pump implementation
```

## Assignments

### Assignment 1: Sensor Data Acquisition

- **ESP-IDF**: BH1750 light sensor (15s intervals)
- **MicroPython**: Multi-sensor system (SCD41, RPR-0521rs, BH1750, DPS310)
- **Features**: Async management, MQTT publishing, high precision

### Assignment 2: MQTT/Kafka Processing (Go)

- **MQTT/Kafka**: Data publishing, subscription, stream processing
- **Analytics**: Rolling average (5-min), CO2 threshold (700ppm), LED control
- **Topics**: `i483/sensors/[ID]/[SENSOR]/[TYPE]`

### Assignment 3: Stream Analytics (Kotlin + Flink)

- **Flink**: Real-time processing (min/max/avg, 5-min windows)
- **Visualization**: Grafana dashboards, Apache IoTDB
- **CEP**: Occupancy detection, multi-sensor fusion
- **Topics**: `i483-sensors-[ID]-analytics-[SENSOR]-[METRIC]-[TYPE]`

## Technical Stack

- **Hardware**: ESP32, SCD41, BH1750, RPR-0521rs, DPS310 sensors
- **Languages**: C (ESP-IDF), Python (MicroPython), Go, Kotlin
- **Messaging**: MQTT, Apache Kafka
- **Analytics**: Apache Flink, Grafana, Apache IoTDB
- **Deployment**: Docker, Docker Compose


## Author

**XU Pengfei (2510082)** - JAIST I483 Course Project 2025
- Special Thanks to my AI Assistant(s): GPT（4o、4.1）, Claude(3.7、4), Gemini-2.5 😄
