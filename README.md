**Kafka Streaming Lakehouse with Airflow and Spark**

**Project Overview**
This project implements an end to end real time data engineering pipeline using Kafka, Apache Spark, and Apache Airflow following a modern Lakehouse architecture. The pipeline ingests streaming order events, processes them through Bronze, Silver, and Gold layers, and produces analytics ready datasets. The entire system is containerized using Docker and orchestrated using Airflow.

The project demonstrates real world data engineering concepts including streaming ingestion, schema enforcement, data quality validation, orchestration, and analytical aggregations.

**Architecture Summary**
The pipeline follows a Lakehouse pattern with clearly separated responsibilities.

   1. Kafka is used for real time ingestion of order events
   2. Spark Structured Streaming processes data incrementally
   3. Data is stored in Bronze, Silver, and Gold layers
   4. Airflow orchestrates the processing workflow
   5. Docker is used for local, reproducible infrastructure

**Technology Stack**
    Apache Kafka for streaming ingestion
    Apache Spark 3.5.5 for data processing
    Apache Airflow 2.9 for orchestration
    PostgreSQL for Airflow metadata
    Docker and Docker Compose for containerization
    Python for producers, Spark jobs, and orchestration logic
    Parquet format for analytical storage

**Data Flow Explanation**

**Kafka Producer**
    A Python Kafka producer continuously generates synthetic order events
    Events include order details such as product, quantity, price, city, and timestamps
    Events are published to a Kafka topic

**Bronze Layer**
    Spark Structured Streaming reads data from Kafka
    Raw events are stored exactly as received
    No transformations are applied
    Data is written in Parquet format
    Acts as the immutable raw data layer

**Silver Layer**
    Spark reads data from the Bronze layer
    Data quality validations are applied
    Invalid records are routed to a quarantine dataset
    Valid records are cleaned and standardized
    Business logic such as total amount calculation is applied

**Gold Layer**
    Spark aggregates Silver data into analytical datasets
    Revenue by city and hour
    Revenue by product and day
    Data is optimized for reporting and analytics

**Orchestration with Airflow**
    Airflow DAG monitors the Bronze layer for new data
    When data is available, Silver processing is triggered
    After Silver completes successfully, Gold processing is triggered
    Tasks are chained with dependencies to ensure correct execution order
    Logs and execution status are visible in the Airflow UI
    
**Trigger Based Execution Design**
    Airflow does not directly run Spark jobs
    Instead, Airflow creates lightweight trigger files
    A separate Spark runner process monitors these trigger files
    When a trigger file appears, the corresponding Spark job is executed
    Trigger files are deleted after consumption to prevent duplicate runs
This design decouples orchestration from execution and mirrors real production patterns.

**Data Storage Structure**
    data/bronze contains raw streaming data
    data/silver contains cleaned and validated data
    data/silver/quarantine contains invalid records
    data/gold contains aggregated analytics datasets
    data/checkpoints stores Spark streaming checkpoints
All data outputs are excluded from Git to keep the repository lightweight.

**Repository Structure**
    producer contains Kafka producer code
    spark/jobs contains Spark batch and streaming jobs
    airflow/dags contains Airflow DAG definitions
    docker contains Docker Compose configuration
    runner.py handles Spark job execution triggered by Airflow
    data folder structure is maintained using placeholder files

**How to Run the Project Locally**
    Start all services using Docker Compose
    Start the Kafka producer to generate streaming data
    Launch the Spark runner process
    Access Airflow UI to monitor DAG execution
    Observe Bronze, Silver, and Gold data generation locally

**Why Data Outputs Are Not in Git**
    Parquet files are large and environment specific
    Outputs are generated dynamically during execution
    Git repository focuses on reproducible code and architecture
    Folder structure is preserved for clarity

**Future Enhancements**
    Add Delta Lake for ACID guarantees
    Add schema evolution handling
    Add monitoring and alerting
    Deploy to cloud storage such as ADLS or S3
    Integrate BI tools for visualization

