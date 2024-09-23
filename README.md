
# Streaming Data Processing Pipeline

## Overview
This pipeline is designed to process real-time streaming data using Kafka, Apache Spark, and Docker for containerization. Apache Airflow is used to orchestrate the process, ensuring the workflow is smooth and repeatable. The processed data is output in the form of reports, which are saved to a specified local file system.

### Architecture

![Pipeline Architecture](./architecture.png)

- **Apache Airflow**: Orchestrates the entire workflow, controlling the scheduling and execution of jobs.
- **Apache Kafka**: Acts as the data streaming source for real-time log ingestion.
- **Python**: Create a python job inside airflow dag to produce kafka topic(**Producer**).
- **Local File System**: Static CSV files are also read by the pipeline.
- **Apache Spark**: Processes the data, performing transformations and aggregations.
- **Docker**: Used for containerizing all components to run them in isolated environments.

## Prerequisites
Before setting up the pipeline, ensure you have the following installed:
- install docker

## Step-by-Step Setup

### Step 1: Setup Docker
1. pull the following branch from github: https://github.com/HassanAli-star/big_data_pipeline

1. Change directory and go inside the big_data_pipeline folder.
3. Run the containers using:
   ```bash
   docker-compose up -d
   ```

### Step 2: Kafka UI
1. After sucessfully created container you can open Kafka UI to see the information about topics, broker, consumer using following url.
   ```
   http://localhost:8089/ui
   ```
   

### Step 3: Airflow UI
1. Login into the Airflow using following URL:
   ```
   http://localhost:8080
   ```
2. user and password are 'airflow'



### Step 4: Spark Streaming Job
1. The Spark job reads from two sources:
   - Real-time data from Kafka (`view_log` topic).
   - Static CSV files from the local file system.
   
2. The Spark job transforms and aggregates data in 1-minute time windows. It uses:
   - Kafka stream input for real-time data processing.
   - CSV for joining static information (campaign metadata).

3. The job then writes the aggregated data as **Parquet files**, partitioned by `network_id` and `minute_timestamp`.

4. To run the Spark job, configure the job in the Airflow DAG or trigger it manually:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark_processing_job.py
   ```

### Step 5: View Processed Reports
1. The output data is written to the local file system in a specified directory (`/app/output/`).
2. The output is partitioned by `network_id` and `minute_timestamp` to allow for easy querying and further analysis.

### Step 6: Stopping the Pipeline
1. To gracefully stop the Spark Streaming job, use:
   ```python
   ssc.stop(stopSparkContext=False, stopGracefully=True)
   ```

2. To stop the Docker containers:
   ```bash
   docker-compose down
   ```

### Future Enhancements
- **Monitoring**: Integrate monitoring tools like Prometheus and Grafana to monitor the Kafka and Spark pipeline.
- **Scalability**: Extend the pipeline to work with multiple Kafka topics and micro-batches.
