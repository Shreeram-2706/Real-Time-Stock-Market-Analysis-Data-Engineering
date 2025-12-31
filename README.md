# 🔥 End-to-End Real-Time Stock Data Engineering Project

I built an end-to-end **real-time stock market data pipeline** using the **Modern Data Stack**.  
In this project, I stream live stock market data, process and transform it through multiple layers, store it in Snowflake, and create interactive dashboards for analysis.

---

## 🚀 Project Overview

In this project, I designed and implemented a complete real-time data engineering pipeline for stock market data.

### What I Did:
- Pulled live stock market data from an external API (FINNHUB)
- Streamed real-time data using Apache Kafka
- Stored raw data in MinIo (Similiar to Amazon S3)
- Loaded data into Snowflake
- Transformed data using DBT following a layered data modeling approach
- Orchestrated the entire workflow using Apache Airflow
- Built interactive dashboards using Power BI (PowerQuery)

---

## 🏗️ Architecture

![Stock Data Pipeline Architecture](https://github.com/Shreeram-2706/Real-Time-Stock-Market-Analysis-Data-Engineering/blob/main/Real%20Time%20Stock%20Market%20Analysis%20Architecture.png)

### Data Flow:
1. I fetch real-time stock market data from an external API  
2. I stream the data using Apache Kafka  
3. I store raw data in an MinIO
4. I load the data into Snowflake for analytics  
5. I transform data using DBT through multiple layers:
   - **Raw Layer**
   - **Cleaned Layer**
   - **Business Ready Layer**
6. I schedule and monitor the pipelines using Apache Airflow  
7. I visualize insights using Power BI  

---

## 🛠️ Tools & Technologies Used

- **Python** – Data ingestion and processing  
- **Apache Kafka** – Real-time data streaming  
- **MinIo** – Raw data storage  
- **Snowflake** – Cloud data warehouse  
- **DBT** – Data transformation and modeling  
- **Apache Airflow** – Workflow orchestration  
- **Power BI** – Data visualization  
- **Docker** – Containerized environment  

---

## Power BI Output

![Power BI Output](https://github.com/Shreeram-2706/Real-Time-Stock-Market-Analysis-Data-Engineering/blob/main/Power%20BI%20Output.png)
