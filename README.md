## 🏠 Practical Data Engineering: A Hands-On Real-Estate Scraping Project Guide

A modular Python project for scraping, cleaning, and structuring real estate listings in Dublin—featuring delta-rs for processing, dbt for transformation, DuckDB for storage, Superset for visualization, and Dagster for orchestration. Actively developed with future plans for Kubernetes integration.

---

## 🌟 About This Project

This repository presents a practical data engineering project that addresses real-world challenges using modern, production-grade tools and workflows. It guides you through building a complete data application that collects real estate listings via web scraping, processes the data using delta-rs, transforms it with dbt, and stores it in DuckDB for analytics. Insights are delivered through interactive visualizations using Apache Superset, and workflows are managed with Dagster.
The project is designed as a learning resource while incorporating comprehensive, real-world use cases. It emphasizes the full lifecycle of a data pipeline—from ingestion to transformation to visualization—making it a valuable foundation for hands-on data engineering practice.
This repository is under active development, with orchestration on Kubernetes planned as part of the future scope.

---

### Features & Learnings
- Scraping real estate listings using a combination of Selenium and Beautiful Soup
- Implementing Change Data Capture (CDC) for efficient data updates
- Using MinIO as an S3-compatible gateway for cloud-agnostic storage
- Performing UPSERTs and maintaining ACID guarantees with Delta Lake via delta-rs
- Transforming and testing data with dbt-core to ensure data quality and maintainability
- Warehousing and querying with DuckDB for fast, local analytics
- Visualizing insights with Apache Superset
- Orchestrating data workflows using Dagster
- Future scope: Deploying on Kubernetes for scalability and portability

---

## 🛠 Tech Stack

- **Language**: Python 3.11  
- **Scraping**: Selenium, undetected-chromedriver  
- **Data Processing**: Polars  
- **Object Storage and Delta Lake**: MinIO (S3-compatible), Delta Lake
- **Data warehouse**: Duckdb
- **Package Manager**: [uv](https://github.com/astral-sh/uv) (ultrafast Python dependency manager)  
- **Other Tools**: pyarrow, boto3

---

## 🗂 Project Structure
```
.
├── pipelines
│   ├── __init__.py
│   ├── delta_lake.py
│   ├── helper_functions.py
│   ├── main.py
│   ├── min_io_ingestion.py
│   ├── minio_upload.py
│   └── scrape_props.py
├── pyproject.toml
├── README.md
├── requirements.txt
├── scraped_data
│   └── __init__.py
├
└── test
    ├── __init__.py
    └── test.py

```

## ⚙️ Installation & Setup

> Requires Python 3.11+, MinIO server, and `uv` installed.

1. **Clone the repository**
   ```bash
   git clone https://github.com/abhibalu/web2warehouse.git
   cd real_estate_pipeline
2. Install dependencies using uv
uv venv
source .venv/bin/activate
uv pip install -r requirements.lock

Configure MinIO access
Set your AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_ENDPOINT_URL as environment variables.

### 🚀 Usage

**Scrape property listing pages:**

```bash
python scraper.py
```
**Convert to .ndjson and upload to MinIO:**
```
python minio_upload.py
```
**Create staging and Cleaned Intermediate Delta Lake table from raw JSON:**
```
python delta_lake.py --layer stg + Intermediate

```

### 🔎 Output

- Staging Layer: Raw nested .parquet files stored in ./data/stg/
- Silver Layer: Cleaned, flattened .parquet files stored in ./data/silver/

### 🔭 Future Work / Roadmap

- ⏳ Add orchestration with Dagster 
- 🧱 Load silver layer into a data warehouse (Druid / DuckDB) - DuckDB is done
- 🚧 Create data models on warehouse using DBT (Currently in dev)
- 📊 Build a dashboard in Apache Superset or Streamlit
- 🧪 Add Pytest unit tests for cleaning + flattening functions



⚠️ Disclaimer
This project is designed for educational purposes, demonstrating web scraping and data engineering practices. Ensure you do not violate any website's copyright or terms of service, and approach scraping responsibly and respectfully.


🙌 Acknowledgments
Built with curiosity, coffee, and way too many open browser tabs.

📣 Feedback
Your feedback is invaluable to improve this project. If you've built your project based on this repository or have suggestions, please let me know through creating an Issues or a Pull Request directly.


