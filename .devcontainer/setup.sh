#!/bin/bash

echo "🚀 Setting up ETL Pipeline Demo..."

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Create required directories
mkdir -p dags logs plugins config

# Set up environment file
echo "AIRFLOW_UID=50000" > .env

# Create demo GCP credentials (placeholder)
echo "Creating demo credentials..."
cat > config/gcp-key.json << 'EOF'
{
  "type": "service_account",
  "project_id": "demo-project",
  "private_key_id": "demo",
  "private_key": "-----BEGIN PRIVATE KEY-----\nDEMO_KEY\n-----END PRIVATE KEY-----\n",
  "client_email": "demo@demo-project.iam.gserviceaccount.com",
  "client_id": "demo",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}
EOF

# Create demo DAG (without BigQuery for demo)
cat > dags/demo_etl_pipeline.py << 'EOF'
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd

def extract_products():
    """Extract products from Fake Store API"""
    print("🛍️ Fetching products from Fake Store API...")
    try:
        response = requests.get('https://fakestoreapi.com/products', timeout=30)
        response.raise_for_status()
        products = response.json()
        
        if len(products) < 10:
            raise ValueError(f"Too few products: {len(products)}")
        
        # Save to local file for demo
        df = pd.DataFrame(products)
        df.to_csv('/tmp/products.csv', index=False)
        
        print(f"✅ Extracted {len(products)} products")
        return len(products)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

def extract_currency():
    """Extract currency rates"""
    print("💱 Fetching currency rates...")
    try:
        response = requests.get('https://api.exchangerate-api.com/v4/latest/USD', timeout=30)
        response.raise_for_status()
        currency_data = response.json()
        
        # Save to local file for demo
        rates_list = []
        for currency, rate in currency_data['rates'].items():
            rates_list.append({'currency': currency, 'rate': rate})
        
        df = pd.DataFrame(rates_list)
        df.to_csv('/tmp/currency_rates.csv', index=False)
        
        print(f"✅ Extracted {len(rates_list)} currency rates")
        return len(rates_list)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

def create_analytics():
    """Create analytics from extracted data"""
    print("📊 Creating analytics...")
    try:
        # Read products data
        df_products = pd.read_csv('/tmp/products.csv')
        
        # Simple analytics
        analytics = {
            'total_products': len(df_products),
            'avg_price': df_products['price'].mean(),
            'categories': df_products['category'].value_counts().to_dict()
        }
        
        # Save analytics
        with open('/tmp/analytics.json', 'w') as f:
            json.dump(analytics, f)
        
        print(f"✅ Analytics created: {analytics}")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

default_args = {
    'owner': 'demo',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'demo_etl_pipeline',
    default_args=default_args,
    description='Demo ETL Pipeline for Portfolio',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['demo', 'portfolio'],
)

extract_products_task = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products,
    dag=dag,
)

extract_currency_task = PythonOperator(
    task_id='extract_currency',
    python_callable=extract_currency,
    dag=dag,
)

analytics_task = PythonOperator(
    task_id='create_analytics',
    python_callable=create_analytics,
    dag=dag,
)

[extract_products_task, extract_currency_task] >> analytics_task
EOF

# Create demo Streamlit app with sample data
cat > streamlit/demo_app.py << 'EOF'
import streamlit as st
import pandas as pd
import plotly.express as px
import json
from datetime import datetime

st.set_page_config(
    page_title="ETL Pipeline Demo",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 ETL Pipeline Demo")
st.markdown("**This is a live demo running in GitHub Codespaces!**")
st.markdown("---")

# Create sample data
sample_products = [
    {"id": 1, "title": "Fjallraven Backpack", "price": 109.95, "category": "men's clothing", "rating": {"rate": 3.9, "count": 120}},
    {"id": 2, "title": "Mens Casual Premium Slim Fit T-Shirts", "price": 22.3, "category": "men's clothing", "rating": {"rate": 4.1, "count": 259}},
    {"id": 3, "title": "Mens Cotton Jacket", "price": 55.99, "category": "men's clothing", "rating": {"rate": 4.7, "count": 500}},
    {"id": 4, "title": "Mens Casual Slim Fit", "price": 15.99, "category": "men's clothing", "rating": {"rate": 2.1, "count": 430}},
    {"id": 5, "title": "John Hardy Women's Chain Bracelet", "price": 695, "category": "jewelery", "rating": {"rate": 4.6, "count": 400}}
]

df = pd.DataFrame(sample_products)
df['rating_score'] = df['rating'].apply(lambda x: x['rate'])
df['rating_count'] = df['rating'].apply(lambda x: x['count'])

# Metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Products", len(df))
with col2:
    st.metric("Avg Price", f"${df['price'].mean():.2f}")
with col3:
    st.metric("Avg Rating", f"{df['rating_score'].mean():.1f}/5")

# Chart
st.subheader("📊 Sample Data Visualization")
fig = px.bar(df, x='category', y='price', title="Price by Category")
st.plotly_chart(fig, use_container_width=True)

st.subheader("📦 Product Data")
st.dataframe(df[['title', 'category', 'price', 'rating_score']], use_container_width=True)

st.markdown("---")
st.markdown("**🔧 Tech Stack:** Apache Airflow • BigQuery • Streamlit • Docker")
EOF

echo "Demo setup complete!"
echo ""
echo "To start the pipeline:"
echo "1. Run: docker-compose up"
echo "2. Wait 2-3 minutes for services to start"
echo "3. Airflow UI: http://localhost:8080 (admin/airflow)"
echo "4. Demo Dashboard: streamlit run streamlit/demo_app.py"