# Real-Time Stock Market Streaming Pipeline with Lakehouse Architecture

A production-grade real-time data engineering pipeline that ingests live stock market tick data, processes it through Apache Kafka and Spark Structured Streaming, stores it in a Delta Lake Medallion Architecture, and orchestrates everything with Apache Airflow.

## Architecture

```
Live Stock Data (Simulated WebSocket)
            ↓
    Apache Kafka (Producer/Consumer)
    5 partitions · offset management · consumer groups
            ↓
    Apache Spark Structured Streaming
    Micro-batch · Watermarking · VWAP · Volatility
            ↓
    Delta Lake — Medallion Architecture
    Bronze (raw) → Silver (cleaned) → Gold (aggregated)
            ↓
    dbt Transformations (Gold layer)
    VWAP · Volatility · Rolling Z-score
            ↓
    Apache Airflow (Orchestration)
    8 DAGs · Health checks · Data quality · Alerts
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 7.4.0 |
| Stream Processing | Apache Spark 3.5.0 Structured Streaming |
| Storage | Delta Lake 3.2.0 — Medallion Architecture |
| Transformation | dbt (Bronze → Silver → Gold) |
| Orchestration | Apache Airflow 2.8.0 |
| Containerization | Docker + Docker Compose |
| Language | Python 3.11 |

## Key Features

- **Real-time VWAP** — Volume Weighted Average Price computed per symbol per minute using Spark window functions
- **Watermark-based late data handling** — 2-minute watermark prevents incorrect aggregations
- **Delta Lake ACID transactions** — 44+ streaming update versions with full time-travel capability
- **Medallion Architecture** — Bronze (raw ticks) → Silver (1-min VWAP) → Gold (daily aggregates)
- **Automated data quality** — Airflow DAGs validate null counts, negative volumes, and symbol coverage
- **Exactly-once delivery** — Kafka consumer groups with offset management
- **Rolling volatility** — 30-minute rolling standard deviation and Z-score per symbol

## Project Structure

```
stock-pipeline/
├── docker-compose.yml          # Kafka, Zookeeper, Airflow services
├── kafka/
│   ├── producer.py             # Simulates live tick data for 5 symbols
│   └── consumer.py             # Reads and aggregates from Kafka topic
├── spark/
│   └── streaming_job.py        # Spark Structured Streaming — Bronze + Silver layers
├── delta/
│   └── lakehouse.py            # Gold layer builder + time travel + optimization
├── dbt_project/
│   ├── dbt_project.yml
│   └── models/
│       ├── bronze/
│       │   └── bronze_trades.sql
│       ├── silver/
│       │   └── silver_trades.sql
│       └── gold/
│           ├── gold_vwap.sql
│           └── gold_volatility.sql
├── airflow/
│   └── dags/
│       └── pipeline_dag.py     # 7-task DAG with health checks and data quality
├── start_pipeline.ps1          # One-click startup script
└── README.md
```

## Delta Lake — Medallion Architecture

| Layer | Path | Description | Format |
|---|---|---|---|
| Bronze | `C:/tmp/delta/bronze/trades` | Raw tick data from Kafka | Delta (append-only) |
| Silver | `C:/tmp/delta/silver/vwap_1min` | 1-minute VWAP aggregations | Delta (append + watermark) |
| Gold | `C:/tmp/delta/gold/daily_vwap` | Daily VWAP summary per symbol | Delta (overwrite) |
| Gold | `C:/tmp/delta/gold/volatility` | 30-min rolling volatility metrics | Delta (overwrite) |

## Airflow Pipeline DAG

```
check_bronze_health
        ↓
validate_silver_quality
        ↓
build_gold_layer
        ↓
run_dbt_transforms
        ↓
run_dbt_tests
        ↓
optimize_delta_tables
        ↓
pipeline_health_report
```

Scheduled every hour. Validates data quality at each stage before proceeding.

## dbt Models

| Layer | Model | Description |
|---|---|---|
| Bronze | `bronze_trades` | Type casting, null filtering, spread calculation |
| Silver | `silver_trades` | 1-minute VWAP, high/low, tick count, stddev |
| Gold | `gold_vwap` | Daily VWAP with volume-weighted aggregation |
| Gold | `gold_volatility` | 30-min rolling stddev, Z-score, volatility flag |

## Live Pipeline Output (Sample)

```
Batch: 28
+------------------------------------------+------+------------------+------------+----------+
|window                                    |symbol|vwap              |total_volume|tick_count|
+------------------------------------------+------+------------------+------------+----------+
|{2026-06-05 06:26:00, 2026-06-05 06:27:00}|MSFT  |411.71            |2,739,598   |548       |
|{2026-06-05 06:42:00, 2026-06-05 06:43:00}|AMZN  |195.46            |2,623,565   |551       |
|{2026-06-05 06:36:00, 2026-06-05 06:37:00}|TSLA  |250.19            |2,836,346   |545       |
|{2026-06-05 06:50:00, 2026-06-05 06:51:00}|GOOGL |125.36            |2,453,429   |492       |
|{2026-06-05 06:49:00, 2026-06-05 06:50:00}|AAPL  |161.48            |2,715,376   |538       |
+------------------------------------------+------+------------------+------------+----------+
```

## How to Run

### Prerequisites

- Python 3.11+
- Docker Desktop
- Java 17 (Eclipse Temurin)
- Hadoop winutils (Windows only)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/YOUR_USERNAME/stock-pipeline.git
cd stock-pipeline

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\activate   # Windows
source .venv/bin/activate  # Mac/Linux

# 3. Install dependencies
pip install pyspark==3.5.0 delta-spark==3.2.0 kafka-python dbt-core apache-airflow

# 4. Set environment variables (Windows)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:JAVA_HOME\bin;C:\hadoop\bin;" + $env:Path
$env:JAVA_TOOL_OPTIONS = "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"

# 5. Start Docker services
docker-compose up -d

# 6. Create Kafka topic
docker exec -it stock-pipeline-kafka-1 kafka-topics --create --topic stock-ticks --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1
```

### Run the Pipeline

Open 3 separate terminals:

```bash
# Terminal 1 — Start Kafka producer
python kafka/producer.py

# Terminal 2 — Start Spark streaming
python spark/streaming_job.py

# Terminal 3 — Build Gold layer
python delta/lakehouse.py
```

### Quick Start (Windows — after first setup)

```powershell
.\start_pipeline.ps1
```

### Run dbt Transformations

```bash
cd dbt_project
dbt run
dbt test
```

### Access Airflow UI

```
URL:      http://localhost:8081
Username: admin
Password: admin123
```

Enable the `stock_pipeline` DAG and trigger a run.


## Key Technical Decisions

**Why Delta Lake over plain Parquet?**
ACID transactions, time-travel queries, and schema evolution make Delta Lake production-ready. Plain Parquet has no transaction guarantees.

**Why Kafka over direct Spark file ingestion?**
Kafka decouples producers from consumers, enabling exactly-once delivery, replay from any offset, and multiple independent consumers on the same stream.

**Why dbt on the Gold layer?**
dbt brings version control, automated testing, and documentation to SQL transformations — treating analytics code with the same rigor as application code.

**Why Airflow for orchestration?**
Airflow provides dependency management between pipeline stages, retry logic on failures, and a visual UI for monitoring — essential for production data pipelines.

## Symbols Tracked

| Symbol | Company | Base Price |
|---|---|---|
| AAPL | Apple Inc. | ~$180 |
| GOOGL | Alphabet Inc. | ~$140 |
| MSFT | Microsoft Corp. | ~$380 |
| TSLA | Tesla Inc. | ~$250 |
| AMZN | Amazon.com Inc. | ~$175 |

## Dataset

Synthetic tick data generated by `kafka/producer.py` simulating real market microstructure:
- Price random walk with ±0.2% per tick
- Volume between 100 and 10,000 shares per tick
- Bid/ask spread of $0.01 to $0.05
- 5 symbols × ~10 ticks/second = ~50 events/second

## License

MIT License — free to use and modify.
