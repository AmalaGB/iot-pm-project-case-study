# iot-pm-project-case-study

IoT Streaming Pipeline (Kafka, Spark, Delta Lake)

This project sets up a basic real-time data pipeline using Kafka, Spark Structured Streaming, and Delta Lake. The goal is to read IoT sensor data from Kafka, process it using Spark, and store the results in Delta format.

Project Structure
iot-pm-project/
docker-compose.yml
spark/
producer/
alerting/
ml/
scoring/
data/
delta/
libs/


Components Used

1) Zookeeper
2)Kafka
3)Spark Master
4)Spark Worker

Delta Lake (used inside Spark job)

How to Start the Pipeline
1. Start the services

Run this inside the project folder:
docker-compose up -d

---This starts Zookeeper, Kafka, Spark Master, and Spark Worker.

2. Check containers
docker ps

You should see:
1) spark-master
2) spark-worker
3) kafka
4) zookeeper

3. Submit the Spark streaming job
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/spark-apps/streaming_pipeline.py


Make sure your Spark script is placed inside:

spark/streaming_pipeline.py
and mounted to the container as:
/opt/spark-apps/

Notes-----

Kafka is available at localhost:9092.
Spark Master UI runs on localhost:8081.
Output tables are written to the delta/ folder.
Logs are generated inside each service’s container.

Stopping Everything
docker-compose down


-To remove everything including volumes:
docker-compose down -v

Purpose of the Project

This setup allows you to test a simple end-to-end data pipeline:

A producer sends JSON messages to Kafka.

Spark streams data from Kafka, processes it, and writes Delta output.

The data can be used for alerting, scoring, and building ML models.
