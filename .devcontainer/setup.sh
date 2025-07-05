#!/bin/bash

echo "Setting up ETL Pipeline Demo Environment..."

# Wait for Docker to be ready
echo "Waiting for Docker daemon to start..."
while ! docker info > /dev/null 2>&1; do
    echo "Docker not ready yet, waiting..."
    sleep 2
done

echo "Docker is ready!"

# Install Python dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install streamlit plotly pandas google-cloud-bigquery db-dtypes

# Create required directories
echo "Creating project directories..."
mkdir -p dags logs plugins config streamlit

# Set up environment file
echo "Setting up environment..."
echo "AIRFLOW_UID=50000" > .env

# Create demo GCP credentials (placeholder for demo)
echo "Creating demo credentials..."
cat > config/gcp-key.json << 'EOF'
{
  "type": "service_account",
  "project_id": "demo-project-12345",
  "private_key_id": "demo123",
  "private_key": "-----BEGIN PRIVATE KEY-----\nDEMO_PRIVATE_KEY_CONTENT_FOR_DEMO_ONLY\n-----END PRIVATE KEY-----\n",
  "client_email": "demo-service@demo-project-12345.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/demo-service%40demo-project-12345.iam.gserviceaccount.com"
}
EOF

# Create demo DAG that works without BigQuery
echo "Creating demo DAG..."
cat > dags/demo_pipeline.py << 'EOF'
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os

def extract_products():
    """Extract products from Fake Store API"""
    print("Fetching products from Fake Store API...")
    try:
        response = requests.get('https://fakestoreapi.com/products', timeout=30)
        response.raise_for_status()
        products = response.json()
        
        if len(products) < 10:
            raise ValueError(f"Too few products: {len(products)}")
        
        # Save to file for demo
        os.makedirs('/tmp/demo_data', exist_ok=True)
        df = pd.DataFrame(products)
        df.to_csv('/tmp/demo_data/products.csv', index=False)
        
        print(f"Extracted {len(products)} products")
        return len(products)
    except Exception as e:
        print(f"Error: {e}")
        raise

def extract_currency():
    """Extract currency rates"""
    print("Fetching currency rates...")
    try:
        response = requests.get('https://api.exchangerate-api.com/v4/latest/USD', timeout=30)
        response.raise_for_status()
        currency_data = response.json()
        
        # Save to file for demo
        rates_list = []
        for currency, rate in list(currency_data['rates'].items())[:20]:  # Limit for demo
            rates_list.append({'currency': currency, 'rate': rate})
        
        df = pd.DataFrame(rates_list)
        df.to_csv('/tmp/demo_data/currency_rates.csv', index=False)
        
        print(f"Extracted {len(rates_list)} currency rates")
        return len(rates_list)
    except Exception as e:
        print(f"Error: {e}")
        raise

def create_analytics():
    """Create analytics from extracted data"""
    print("Creating analytics...")
    try:
        # Read the extracted data
        products_df = pd.read_csv('/tmp/demo_data/products.csv')
        currency_df = pd.read_csv('/tmp/demo_data/currency_rates.csv')
        
        # Create analytics
        analytics = {
            'total_products': len(products_df),
            'avg_price': float(products_df['price'].mean()),
            'categories': products_df['category'].value_counts().to_dict(),
            'total_currencies': len(currency_df),
            'sample_rates': currency_df.head(5).to_dict('records')
        }
        
        # Save analytics
        with open('/tmp/demo_data/analytics.json', 'w') as f:
            json.dump(analytics, f, indent=2)
        
        print(f"Analytics created: {analytics}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        raise

default_args = {
    'owner': 'demo',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'demo_etl_pipeline',
    default_args=default_args,
    description='Demo ETL Pipeline for GitHub Codespaces',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['demo', 'portfolio', 'codespaces'],
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

# Create demo Streamlit app that reads from the pipeline output
cat > streamlit/codespaces_demo.py << 'EOF'
import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
from datetime import datetime

st.set_page_config(
    page_title="ETL Pipeline Codespaces Demo",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 ETL Pipeline Live Demo")
st.markdown("**Running live in GitHub Codespaces!**")

# Check if pipeline has run
data_dir = '/tmp/demo_data'
if os.path.exists(f'{data_dir}/products.csv'):
    st.success("Pipeline has extracted data successfully!")
    
    # Load real pipeline output
    products_df = pd.read_csv(f'{data_dir}/products.csv')
    currency_df = pd.read_csv(f'{data_dir}/currency_rates.csv')
    
    if os.path.exists(f'{data_dir}/analytics.json'):
        with open(f'{data_dir}/analytics.json', 'r') as f:
            analytics = json.load(f)
        
        # Show metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Products Extracted", analytics['total_products'])
        with col2:
            st.metric("Average Price", f"${analytics['avg_price']:.2f}")
        with col3:
            st.metric("Currency Rates", analytics['total_currencies'])
        
        # Show charts
        st.subheader("Category Distribution")
        category_data = pd.DataFrame(list(analytics['categories'].items()), 
                                   columns=['Category', 'Count'])
        fig = px.bar(category_data, x='Category', y='Count')
        st.plotly_chart(fig, use_container_width=True)
        
        # Show data tables
        st.subheader("Products Data")
        st.dataframe(products_df.head(10), use_container_width=True)
        
        st.subheader("Currency Rates")
        st.dataframe(currency_df, use_container_width=True)
    
else:
    st.warning("Pipeline hasn't run yet. Go to Airflow and trigger the 'demo_etl_pipeline' DAG!")
    st.info("Airflow UI should be available at the forwarded port 8080")

st.markdown("---")
st.markdown("**Tech Stack:** Apache Airflow • Python • Streamlit • Docker • GitHub Codespaces")
EOF

echo ""
echo "Demo environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Run: docker compose up -d"
echo "2. Wait 2-3 minutes for Airflow to start"
echo "3. Go to Airflow UI (port 8080) - login: admin/airflow"
echo "4. Trigger the 'demo_etl_pipeline' DAG"
echo "5. Run: streamlit run streamlit/codespaces_demo.py"
echo "6. View the dashboard with real pipeline data!"
echo ""
echo "Ports will be automatically forwarded in Codespaces"
EOF

chmod +x .devcontainer/setup.sh

echo "Setup script ready!"
echo "Don't forget to update your .devcontainer/devcontainer.json with the new configuration!"