# Real-Time Stock & Crypto Data ETL Pipeline

## Overview

This project implements a high-performance, scalable ETL pipeline designed to ingest **real-time trade data** for stocks and cryptocurrencies from the **Finnhub.io WebSocket API**. The raw data is streamed through **Kafka**, transformed into **1-minute OHLCV (Open, High, Low, Close, Volume) bars** using **pandas**, and persistently stored in a **TimescaleDB** (PostgreSQL extension optimized for time-series) database for efficient querying and analysis.

Built using a **decoupled producer-consumer architecture**, this pipeline demonstrates key data engineering skills including real-time ingestion, message queuing via Kafka, efficient time-series transformation, robust database loading with upserts, and containerized deployment using Docker.

## Architecture

[Finnhub WebSocket] -> [Python Producer (producer.py)] -> [Kafka Topic: raw_trades] -> [Python Consumer (etl_worker.py)] -> [Pandas Transform (transform.py)] -> [TimescaleDB Loader (loader.py)] -> [TimescaleDB Table: stock_bars_1min]


* **Producer:** Connects to Finnhub, receives raw trades, sends them to Kafka.
* **Kafka:** Acts as a buffer, decoupling producer and consumer.
* **Consumer (ETL Worker):** Reads trades from Kafka in batches, orchestrates transformation and loading.
* **Transform:** Aggregates batches of raw trades into 1-minute OHLCV bars.
* **Loader:** Upserts the OHLCV bars into the TimescaleDB database, handling potential duplicates.

## Tech Stack

* **Data Ingestion:** Python, `websocket-client`
* **Message Broker:** Apache Kafka (`kafka-python`)
* **Data Transformation:** `pandas`
* **Database:** TimescaleDB (via `timescale/timescaledb` Docker image) on PostgreSQL 14
* **Database Connector:** `SQLAlchemy`, `psycopg2-binary`
* **Configuration:** `python-dotenv`
* **Containerization:** Docker, Docker Compose
* **Testing:** `pytest`

## Key Features & Accomplishments

* **Built** a real-time ETL pipeline processing live Finnhub trades end-to-end.
* **Implemented** a decoupled producer-consumer architecture using Kafka for scalability and resilience.
* **Developed** efficient time-series aggregation logic using pandas `resample` to create 1-minute OHLCV bars.
* **Ensured** data integrity in TimescaleDB by creating a robust SQL upsert loader function.
* **Containerized** the entire multi-service application (Python, Kafka, Zookeeper, DB) using Docker and Docker Compose for easy, one-command deployment.
* **Validated** core transformation logic with unit tests using `pytest`.

---

## Setup & Installation

**Prerequisites:**
* Docker Desktop installed and running.
* Git installed.
* A free API key from [Finnhub.io](https://finnhub.io/register).

**1. Clone the Repository**
```bash
git clone [https://github.com/Viiraj7/realtime-trading-pipeline.git](https://github.com/Viiraj7/realtime-trading-pipeline.git)
cd realtime-trading-pipeline
2. Set Up Environment Variables

Create a file named .env in the root directory.

Copy the contents from .env.example into .env.

Add your real Finnhub API key to the .env file:

Ini, TOML

FINNHUB_API_KEY="YOUR_ACTUAL_FINNHUB_KEY_HERE"
# --- Keep the rest of the defaults below ---
DB_USER=postgres
DB_PASSWORD=mysecretpassword
DB_HOST=postgres # Service name in docker-compose
DB_PORT=5432
DB_NAME=market_data
KAFKA_SERVER=kafka:29092 # Service name in docker-compose
3. (Optional) Python Virtual Environment for Local Development/Testing (Not needed to run with Docker)

Bash

python -m venv venv
# On macOS/Linux: source venv/bin/activate
# On Windows: .\venv\Scripts\activate
pip install -r requirements.txt
# To run tests: pytest
How to Run
Make sure Docker Desktop is running.

Open your terminal in the project's root directory (realtime-trading-pipeline).

Run the following command:

Bash

docker-compose up --build
--build: Rebuilds the Python Docker image if needed.

Add --force-recreate to ensure containers start fresh.

Observe the logs from the producer (receiving trades) and etl_worker (processing batches and loading to DB).

How to Verify It's Working
Check Logs: Ensure producer shows "Received trade..." and etl_worker shows "Processing batch...", "Transformed...", and "Upsert command executed...".

Check Database:

While the pipeline runs, open a new terminal.

Connect to the PostgreSQL container:

Bash

docker exec -it postgres psql -U postgres -d market_data
Check for recent data:

SQL

SELECT symbol, timestamp, open, high, low, close, volume
FROM stock_bars_1min
ORDER BY timestamp DESC
LIMIT 10;
Filter for a specific symbol:

SQL

SELECT * FROM stock_bars_1min
WHERE symbol = 'BINANCE:BTCUSDT'
ORDER BY timestamp DESC
LIMIT 5;
Type \q to exit psql.

How to Stop
Go to the terminal where docker-compose up is running.

Press Ctrl + C.

(Recommended) Clean up: docker-compose down. Database data is saved in a Docker volume.