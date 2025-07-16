# 🏠 Dublin Property Scraper

A modular Python tool for scraping, cleaning, and structuring real estate listing data in Dublin — built to explore the housing market and implement modern data engineering practices along the way.

---

## 🧭 Motivation / Background

After moving to Dublin and taking some time between jobs, I found myself naturally curious about the local housing market. Rather than browse listings manually, I turned that curiosity into a hands-on project — applying Python, web scraping, and data engineering tools to build a structured, queryable dataset of property listings. What started as a learning exercise grew into a useful tool and a clean, modular project.

---

## ✨ Features

- Automated web scraping of paginated listing pages using Selenium
- Extraction of structured listing metadata in JSON format
- Staging `.ndjson` storage using MinIO-compatible object storage
- Cleaning and flattening of nested fields (e.g., address, price, accommodation summary)
- Conversion to columnar `.parquet` files and Delta Lake format
- Implementing UPSERTs and ACID transactions with Delta Lake and warehouse
- Adapted medallion-style architecture

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


