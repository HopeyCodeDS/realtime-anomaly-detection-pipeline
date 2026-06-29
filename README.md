# Real-Time Anomaly Detection Pipeline

## 📌 Overview
The **Real-Time Anomaly Detection Pipeline** is a containerized, distributed streaming system that ingests live sensor metrics, evaluates them for irregular behavior in-flight using unsupervised machine learning, and stores the results for immediate analysis. 

This project serves as an end-to-end blueprint for building production-grade data pipelines that bridge streaming data infrastructure with real-time predictive analytics.

---

## 🛠 Technologies
* **Infrastructure & Containerization**: Docker, Docker Compose
* **Message Ingestion**: Apache Kafka (v7.4.0 via Confluent), Apache Zookeeper
* **Stream Processing Engine**: Apache Spark (v3.4.0) with PySpark Structured Streaming
* **Machine Learning Engine**: Scikit-Learn (Isolation Forest)
* **Storage & Indexing**: Elasticsearch (v7.17.26)
* **Visualization Layer**: Kibana (v7.17.26)
* **Downstream Integration**: Python Flask REST API

---

## 🧠 Problem Description
In enterprise environments like industrial IoT monitoring, predictive maintenance, and cloud infrastructure operations, waiting for end-of-day batch processing to catch hardware failures is too slow and costly. 

To address this, this project tackles the challenge of **in-flight anomaly detection**. It continuously monitors high-frequency variables (temperature, humidity, pressure, and vibration) and instantly flags abnormal operational behavior within seconds of the event occurring—ensuring immediate visibility before a catastrophic system breakdown happens.

---

## 🏗️ System Architecture & Data Flow
The pipeline is decoupled into separate, highly specialized services running inside an isolated Docker bridge network:

```text
┌────────────────┐      ┌────────────────┐      ┌────────────────┐
│                │      │                │      │                │
│    PRODUCER    │─────>│  APACHE KAFKA  │─────>│ SPARK WORKERS  │
│  (Data Gen &   │      │ (Ingestion &   │      │ (Micro-Batch   │
│ Anomaly Spike) │      │ Message Queue) │      │  Stream Engine)│
└────────────────┘      └────────────────┘      └───────┬────────┘
                                                        │
                                                        │ runs ML inference
                                                        ▼
┌────────────────┐      ┌────────────────┐      ┌────────────────┐
│     KIBANA     │<─────│ ELASTICSEARCH  │<─────│  SPARK MASTER  │
│ (Real-Time UI  │      │(Anomaly Filter │      │(Isolation-     │
│  Dashboards)   │      │   & Indexer)   │      │ Forest Model)  │
└────────────────┘      └───────┬────────┘      └────────────────┘
                                │
                                │ fetches JSON
                                ▼
                        ┌────────────────┐
                        │   FLASK API    │
                        │ (/anomalies &  │
                        │ /api/anomalies)│
                        └────────────────┘


```

### Advanced Infrastructure Enhancements Implemented:
* **JVM Container Optimization**: Injected `-XX:-UseContainerSupport` to bypass modern Linux Kernel `cgroupv2` compatibility bugs during JVM startup.
* **Dynamic Driver Binding**: Managed explicit `PYSPARK_SUBMIT_ARGS` coordinates to programmatically attach the Scala `spark-sql-kafka` connector directly into the streaming container runtime.
* **Write-Permission Isolation**: Re-routed Apache Ivy dependency build environments using `-Divy.home=/tmp/ivy` to eliminate root permissions errors across independent nodes.

---

## ⚙️ Project Features
* **Automated Data Simulation**: Multi-threaded Python producer modeling realistic baseline sensor fluctuations alongside randomized machine failure spikes.
* **Online Machine Learning**: Real-time feature extraction and scoring utilizing an unsupervised Isolation Forest model capable of operating over micro-batches without manual thresholds.
* **Smart Storage Footprint**: High-efficiency storage strategy that processes all telemetry data but selectively persists *only* flagged anomaly records to Elasticsearch.
* **Dual Presentation Layer**: Features both a graphic UI dashboard (Kibana) for operations teams and an programmatic access tier (REST API) for automated down-stream alerts.

---

## 📦 Deliverables
* **IoT Data Producer Simulator**: Python scripts simulating real-time Kafka topics.
* **Spark Streaming Job Engine**: PySpark scripts managing streaming schemas, processing thresholds, and custom `foreachBatch` model inferences.
* **Elasticsearch Index Schemes**: Pre-configured JSON data mappings optimized for querying geographical or multi-variable sensor structures.
* **Validated REST Service**: Production Flask environment providing live data serialization.
* **Orchestration Matrix**: Multi-container `docker-compose.yml` environment containing tailored resource controls and fixed JVM environment variables.

---

## 🚀 Getting Started

### Prerequisites
* Ensure **Docker** and **Docker Compose** are installed and running locally on your machine.

### Deploy the Pipeline
1. Clone the project repository and navigate to its directory:
```bash
git clone https://github.com/HopeyCodeDS/realtime-anomaly-detection-pipeline
cd real-time-anomaly-detection-pipeline
```
   * Build and launch the containerized cluster in detached mode:
```bash
    docker compose up --build -d
```    
---

2. Accessing the Outputs

Interactive UI Dashboards: Open your browser to http://localhost:5601. Navigate to Stack Management > Data Views, generate a view for anomalies*, and view live charts within the Discover module.

REST API Payloads: Query your backend data payloads directly via the browser or command line using the confirmed endpoints:
```bash
    
    http://localhost:5001/anomalies

    http://localhost:5001/api/anomalies

```        