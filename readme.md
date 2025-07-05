# 🚀 ETL Pipeline - Ecommerce shop data

> **Production-ready data pipeline built with Apache Airflow, BigQuery, and Streamlit**

## Local demo 
```bash
git clone https://github.com/fcallerafilho/ecom-shop-etl.git
cd etl-project
docker-compose up
```

## 🏗️ Architecture

```
APIs (Fake Store, Currency) → Airflow DAGs → BigQuery (Raw/Staging/Marts) → Streamlit Dashboard
```

## 📊 Pipeline Flow

### 1. **Data Extraction**

- Fetches product data from Fake Store API
- Retrieves real-time currency exchange rates

### 2. **Data Quality & Staging**

- Cleans and standardizes formats
- Creates staging tables with business rules

### 3. **Analytics Marts**

- Builds analytics-ready tables
- Pre-calculates key metrics

### 4. **Visualization**

- Interactive Streamlit dashboard
- Real-time filtering and analysis
- Production-ready UI/UX
