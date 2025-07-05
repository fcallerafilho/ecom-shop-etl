# 🚀 Multi-Source ETL Pipeline with Real-Time Analytics

> **Production-ready data pipeline built with Apache Airflow, BigQuery, and Streamlit**

## 🎯 Quick Demo

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/yourusername/your-repo-name)

### Or local setup

```bash
git clone https://github.com/yourusername/your-repo-name.git
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
