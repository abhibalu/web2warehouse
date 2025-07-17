## 🏠 Practical Data Engineering: A Hands-On Real-Estate Scraping Project Guide

A modular Python project for scraping, cleaning, and structuring real estate listings in Dublin—featuring delta-rs for processing, dbt for transformation, DuckDB for storage, Superset for visualization, and Dagster for orchestration. Actively developed with future plans for Kubernetes integration.

---

## 🌟 About This Project

This repository presents a practical data engineering project that addresses real-world challenges using modern, production-grade tools and workflows. It guides you through building a complete data application that collects real estate listings via web scraping, processes the data using delta-rs, transforms it with dbt, and stores it in DuckDB for analytics. Insights are delivered through interactive visualizations using Apache Superset, and workflows are managed with Dagster.
The project is designed as a learning resource while incorporating comprehensive, real-world use cases. It emphasizes the full lifecycle of a data pipeline—from ingestion to transformation to visualization—making it a valuable foundation for hands-on data engineering practice.
This repository is under active development, with orchestration on Kubernetes planned as part of the future scope.

---
```mermaid
---
config:
  layout: dagre
---

flowchart LR
  subgraph Raw_Layer["Raw Layer"]
    A2["Structured & Semi-structured JSON Output"]
    B1["MinIO - Staging Bucket (raw JSON)"]
    B2["MinIO - Delta Bucket (Parquet files)"]
  end

  subgraph Bronze_Silver_Layer["Bronze / Silver Layer"]
    B3["Delta Lake (delta-rs)"]
  end

  subgraph Gold_Layer["Gold Layer"]
    C1["DuckDB (Data Warehouse)"]
    C2["dbt-core (Transformations)"]
    C3["Apache Superset (BI Dashboards)"]
  end

  A1["Website Scraper (Selenium + BS4)"] --> A2
  A2 --> B1
  B1 --> B2
  B2 -- "Ingest via delta-rs" --> B3
  B3 -- "Materialized Views" --> C1
  C1 -- "Transformed by" --> C2
  C2 --> C3
  D1["Dagster (Orchestration Engine)"] --> A1 & B3 & C2
  E1["delta-rs (CDC & ACID Writes)"] --> B3
  C3 --> X1["📊 BI Tools"] & X2["📄 Automated Reports"]
  C1 --> X3["🤖 ML Models"]

  %% Logos
  DBT_LOGO[" "]
  DBT_LOGO@{ img: "https://www.biztory.com/hubfs/dbt-signature_logo.png", h: 100, w: 100, pos: "b" }
  DBT_LOGO -.-> C2

  DELTA_LOGO[" "]
  DELTA_LOGO@{ img: "https://miro.medium.com/v2/resize:fit:720/format:webp/1*6oNE5oCiddsL01fkMHISVg.png", h: 100, w: 100, pos: "t" }
  DELTA_LOGO -.-> Bronze_Silver_Layer

  MINIO_LOGO[" "]
  MINIO_LOGO@{ img: "https://blog.min.io/content/images/size/w2000/2019/05/0_hReq8dEVSFIYJMDv.png", label: "MinIO", pos: "t", w: 80, h: 40, constraint: "on" }
  MINIO_LOGO -.-> Raw_Layer

  DUCKDB_LOGO[" "]
  DUCKDB_LOGO@{ img: "https://camo.githubusercontent.com/53a363f1189b7942f9a3f7801f79e72fe90758ff302473513f2532c4db965e69/68747470733a2f2f6475636b64622e6f72672f696d616765732f6c6f676f2d646c2f4475636b44425f4c6f676f2d686f72697a6f6e74616c2e737667", h: 100, w: 100, pos: "t" }
  DUCKDB_LOGO -.-> Gold_Layer

  DAGSTER_LOGO[" "]
  DAGSTER_LOGO@{ img: "https://dagster-website.vercel.app/images/brand/logos/dagster-primary-horizontal.jpg ", h: 100, w: 100, pos: "b" }
  DAGSTER_LOGO -.-> D1

  SUPERSET_LOGO[" "]
  SUPERSET_LOGO@{ img: "https://ai4sme.aisingapore.org/wp-content/uploads/2022/12/image10.png ", h: 100, w: 100, pos: "b" }
  SUPERSET_LOGO -.-> X1  
```
### Features & Learnings
- Scraping real estate listings using a combination of **Selenium and Beautiful Soup**
- Implementing **Change Data Capture (CDC)** for efficient data updates
- Using **MinIO** as an S3-compatible gateway for cloud-agnostic storage
- Performing UPSERTs and maintaining ACID guarantees with **Delta Lake via delta-rs**
- Transforming and testing data with **dbt-core** to ensure data quality and maintainability
- Warehousing and querying with **DuckDB** for fast, local analytics
- Visualizing insights with **Apache Superset**
- Orchestrating data workflows using **Dagster**
- Future scope: Deploying on **Kubernetes** for scalability and portability

---

## 🛠 Tech Stack
This project leverages a rich set of open-source technologies including Selenium, Polars, delta-rs, MinIO, DuckDB, dbt-core, Apache Superset, and Dagster—with future plans to run on Kubernetes for scalable, cloud-agnostic deployment.

- **Language**: Python 3.11  
- **Scraping**: Selenium, undetected-chromedriver  
- **Data Processing**: Polars  
- **Object Storage & Table Format**: MinIO (S3-compatible), delta-rs (Delta Lake in Rust)  
- **Data Transformation & Testing**: dbt-core  
- **Data Warehouse**: DuckDB  
- **Visualization**: Apache Superset  
- **Workflow Orchestration**: Dagster  
- **Package Manager**: [uv](https://github.com/astral-sh/uv) – ultrafast Python dependency manager  
- **Other Tools**: pyarrow, boto3  

---

## ⚙️ Installation & Setup

> Requires Python 3.11+, MinIO server, and `uv` installed.

1. **Clone the repository**
   ```bash
   git clone https://github.com/abhibalu/web2warehouse.git
   cd real_estate_pipeline
2. **Install dependencies using uv**
```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```
3. **Set your MinIO credentials and endpoint as environment variables:**
```bash
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_ENDPOINT_URL=http://localhost:9000
```
### 🚀 Usage

Before running, ensure that the MinIO server is up and running, and DuckDB is initialized.

Then, run the entire pipeline with:

```bash
make run_all
```

### 🔎 Output

This project follows the **Medallion Architecture** pattern for organizing data into multiple layers:

- **Raw Layer**: The initial landing zone containing raw data ingested from upstream sources. Data is stored as-is in the cloud storage or raw tables without transformations.

- **Bronze Layer**: Applies basic data hygiene such as data type casting, renaming columns for consistency, and schema enforcement on the raw data.

- **Silver Layer**: Transforms the cleaned bronze data into structured facts and dimension tables, making the data analytics-ready.

- **Gold Layer**: (Optional) Builds aggregated tables or One Big Table (OBT) with pre-computed metrics tailored for direct consumption by BI tools or end users.

Each layer progressively refines the data quality and structure to support scalable, maintainable, and performant analytics workflows.


### 🔭 Future Work / Roadmap

- ⏳ Add orchestration with Dagster (Currently in dev)
- 🧱 Load silver layer into a data warehouse (Druid / DuckDB)
- 🚧 Create data models on warehouse using DBT (Currently in dev)
- 📊 Build a dashboard in Apache Superset
- 📦 Containerize the application using Kubernetes for scalability and portability



⚠️ Disclaimer
This project is designed for educational purposes, demonstrating web scraping and data engineering practices. Ensure you do not violate any website's copyright or terms of service, and approach scraping responsibly and respectfully.


🙌 Acknowledgments
Built with curiosity, coffee, and way too many open browser tabs.

📣 Feedback
Your feedback is invaluable to improve this project. If you've built your project based on this repository or have suggestions, please let me know through creating an Issues or a Pull Request directly.


