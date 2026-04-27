# Weather_analysis
A production-grade, end-to-end weather data pipeline built on **Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold). It ingests raw multi-source weather CSV files, applies streaming ingestion, data quality checks, and produces five analytical Gold tables for business consumption.

---

## 📐 Architecture Overview

```
Raw CSVs (Cloud Storage)
        │
        ▼
┌─────────────────────┐
│   BRONZE LAYER      │  Auto Loader (cloudFiles) · Schema Evolution · Streaming
│  bronze_weather_raw │  sensor_payload JSON parsing · Partitioned by load_date
└─────────────────────┘
        │  Change Data Feed (CDF)
        ▼
┌─────────────────────┐
│   SILVER LAYER      │  Data Quality · Type Casting · Station Enrichment
│ silver_weather_clean│  Deduplication · Merge (upsert) · Quarantine · Audit Log
└─────────────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────┐
│                    GOLD LAYER                        │
│  gold_city_weather_snapshot   gold_heatwave_alerts   │
│  gold_rainfall_streaks        gold_temperature_change│
│  gold_provider_quality                               │
└──────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
weather-pipeline/
│
├── 01_Bronze_Ingestion.ipynb        # Stream raw CSVs → bronze_weather_raw
├── 02_silver_transformation.ipynb   # CDF → clean/quarantine + audit
├── 03_Gold_aggregation.ipynb        # 5 business-ready Gold tables
│
└── README.md
```

---

## 🗂️ Delta Tables

| Layer  | Table Name                      | Description                                     |
|--------|---------------------------------|-------------------------------------------------|
| Bronze | `bronze_weather_raw`            | Raw ingested records with metadata              |
| Bronze | `station_master`                | Reference table for weather stations            |
| Silver | `silver_weather_clean`          | Validated, enriched, deduplicated records       |
| Silver | `silver_weather_quarantine`     | Records that failed Silver quality checks       |
| Silver | `pipeline_control`              | Watermark table tracking last processed version |
| Silver | `weather_change_audit`          | Column-level audit log for updated records      |
| Gold   | `gold_city_weather_snapshot`    | Latest weather state per city                   |
| Gold   | `gold_heatwave_alerts`          | Cities with 3+ heatwave days in last 7 days     |
| Gold   | `gold_rainfall_streaks`         | Longest continuous rainfall streak per city     |
| Gold   | `gold_temperature_change`       | Cities with >8 °C swing within 6 hours          |
| Gold   | `gold_provider_quality`         | Data quality score per data provider            |

---

## 🔄 Notebook Details

### 01 — Bronze Ingestion

- Loads the `station_master` reference table from CSV.
- Reads raw weather CSVs from a cloud volume using **Auto Loader** with schema evolution.
- Parses the `sensor_payload` JSON column to back-fill missing numeric fields.
- Adds metadata columns: `ingestion_time`, `source_file`, `batch_id`, `load_date`, `record_status`.
- Streams data into `bronze_weather_raw` (Delta, partitioned by `load_date`, CDF enabled) using `trigger(availableNow=True)`.

### 02 — Silver Transformation

- Reads Bronze incrementally via **Change Data Feed** using a `pipeline_control` watermark.
- Casts all fields to correct types and joins with `station_master` for zone enrichment.
- Applies data quality rules and routes records to `silver_weather_clean` or `silver_weather_quarantine`.

  | Quarantine Rule             | Condition                                      |
  |-----------------------------|------------------------------------------------|
  | `invalid_timestamp`         | `event_time` cannot be parsed                  |
  | `out_of_range_temperature`  | Temperature < −20 °C or > 60 °C               |
  | `out_of_range_humidity`     | Humidity < 0 or > 100                         |
  | `out_of_range_wind_speed`   | Wind speed < 0                                 |
  | `station_not_in_master`     | Station ID not found in reference table        |
  | `data_type_conflict`        | Field present but cannot be cast               |

- Deduplicates by `city + event_time` (latest `ingestion_time` wins).
- Upserts into `silver_weather_clean` via Delta `MERGE`.
- Adds derived columns: `temperature_band`, `weather_severity_score`, `event_date`, `event_hour`.
- Writes column-level change records to `weather_change_audit`.
- Updates `pipeline_control` watermark on success.

### 03 — Gold Aggregation

Reads from `silver_weather_clean` and produces five analytical tables:

| Gold Table                    | Business Question                                                  |
|-------------------------------|--------------------------------------------------------------------|
| `gold_city_weather_snapshot`  | What is the current weather condition in each city right now?      |
| `gold_heatwave_alerts`        | Which cities crossed 40 °C at least 3 times in the last 7 days?  |
| `gold_rainfall_streaks`       | Which city had the longest uninterrupted streak of non-zero rainfall? |
| `gold_temperature_change`     | Which cities had a temperature swing > 8 °C within 6 hours?      |
| `gold_provider_quality`       | Which data provider sends the most unreliable data?               |

---

## ⚙️ Setup & Configuration

### Prerequisites

- Databricks workspace (Runtime **13.x LTS** or higher recommended)
- Unity Catalog enabled
- A catalog named `weather_project` with schema `default`
- A Volume at `/Volumes/weather_project/default/` containing:
  - `raw/` — folder of raw weather CSV files
  - `reference/` — station master CSV file

### Unity Catalog Objects

```sql
CREATE CATALOG IF NOT EXISTS weather_project;
USE CATALOG weather_project;
CREATE SCHEMA IF NOT EXISTS default;
CREATE VOLUME IF NOT EXISTS weather_project.default.raw;
CREATE VOLUME IF NOT EXISTS weather_project.default.reference;
CREATE VOLUME IF NOT EXISTS weather_project.default.checkpoints;
```

### Running the Pipeline

Run the notebooks in order:

```
1. 01_Bronze_Ingestion.ipynb
2. 02_silver_transformation.ipynb
3. 03_Gold_aggregation.ipynb
```

> **Note:** To reset the pipeline for a clean run, uncomment the reset cells at the top of each notebook.

---

## 🔑 Key Configuration Variables

All notebooks share these configuration constants — update them if your catalog or schema name differs:

```python
CATALOG = "weather_project"
SCHEMA  = "default"
```

---

## 🧹 Data Quality Features

- **Schema Evolution** via Auto Loader — new columns are added automatically.
- **Sensor Payload Fallback** — missing fields are back-filled from JSON `sensor_payload`.
- **Quarantine Pattern** — bad records are isolated and never lost.
- **Deduplication** — window-based row-number dedup before Silver merge.
- **CDF Watermarking** — only new Bronze versions are processed each Silver run.
- **Column-Level Audit Trail** — every field change is logged with old/new values.

---

## 📦 Requirements

See [`requirements.txt`](./requirements.txt) for the Python dependencies.  
All core Spark and Delta Lake libraries are pre-installed on Databricks; `requirements.txt` lists what is needed for local development or CI validation.

---

## 📄 License

This project is for educational and portfolio purposes.
