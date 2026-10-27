# 📦 Real-Time Supply Chain Intelligence & Stream Processing Pipeline

An end-to-end, event-driven Big Data engineering pipeline that ingests high-velocity supply chain transaction events, detects operational anomalies in micro-batches, stores aggregated telemetry in PostgreSQL, and serves interactive operational dashboards via Streamlit.

---

## 🏗️ System Architecture

`Data Generator (Producer)` ➡️ `Apache Kafka (Broker)` ➡️ `Apache Spark (Structured Streaming)` ➡️ `PostgreSQL (ACID Storage)` ➡️ `Streamlit (Operations UI)`

* **Event Ingestion**: Decoupled multi-regional event streaming using Apache Kafka topics.
* **Micro-Batch Processing**: Windowed aggregation (5.0s triggers) and rule-based anomaly detection with Apache Spark Structured Streaming.
* **Durability & Fault Tolerance**: Checkpoint persistence with write-ahead logs (`checkpoints/supply_chain`) ensuring deterministic offset recovery.
* **Serving Layer**: Relational ACID storage in PostgreSQL feeding an interactive Streamlit operations center.

---

## 📊 Anomaly Detection & Business Rules

* **Price Spike Alert**: `unit_price > 400.0 INR` (Monitors sudden cost anomalies or capital risks).
* **Logistics SLA Delay**: `delivery_time > 10 days` (Flags supply chain transit bottlenecks).
* **Compound Critical Anomaly**: Flagged when both pricing and transit thresholds are breached simultaneously.

---

## ⚡ Empirical Benchmarking

Evaluated across three distinct workloads (Low, Medium, High Load):
* **Max Sustained Throughput**: 249.15 events/second
* **Mean Processing Latency**: 0.76s (Low) to 2.28s (High)
* **P99 Peak Latency**: 3.53s (Strictly within the 5.0s micro-batch window)
* **State Recovery Latency**: 2.14s with 0.0% data loss

---

## 🚀 Setup & Execution

### Prerequisites
* Docker & Docker Compose
* Python 3.10+

### Quick Start
1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/BigDataStreamProject.git
   cd BigDataStreamProject