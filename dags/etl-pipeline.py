from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

def extract_and_load_products():
    """Extract products with error handling and quality checks"""
    print("🛍️ Fetching products from Fake Store API...")
    
    try:
        # Extract with timeout and error handling
        response = requests.get('https://fakestoreapi.com/products', timeout=30)
        response.raise_for_status()
        products = response.json()
        
        # Data quality check
        if len(products) < 10:
            raise ValueError(f"Too few products returned: {len(products)}. Expected at least 10.")
        
        print(f"✅ Quality check passed: {len(products)} products retrieved")
        
        # Convert to DataFrame
        df = pd.DataFrame(products)
        df['extracted_at'] = datetime.now()
        
        # Load to BigQuery with explicit credentials
        credentials = service_account.Credentials.from_service_account_file(
            '/opt/airflow/config/gcp-key.json'
        )
        client = bigquery.Client(credentials=credentials)
        table_id = "etl-project-465015.raw_data.products"
        
        job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        ))
        job.result()
        
        print(f"✅ Loaded {len(products)} products to BigQuery")
        return len(products)
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API call failed: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise

def extract_and_load_currency():
    """Extract currency with error handling and quality checks"""
    print("💱 Fetching currency rates...")
    
    try:
        # Extract with timeout and error handling
        response = requests.get('https://api.exchangerate-api.com/v4/latest/USD', timeout=30)
        response.raise_for_status()
        currency_data = response.json()
        
        # Data quality check
        if 'rates' not in currency_data or len(currency_data['rates']) < 50:
            raise ValueError(f"Invalid currency data: {len(currency_data.get('rates', []))} rates")
        
        print(f"✅ Quality check passed: {len(currency_data['rates'])} currency rates")
        
        # Convert rates to DataFrame
        rates_list = []
        for currency, rate in currency_data['rates'].items():
            rates_list.append({
                'currency': currency,
                'rate': rate,
                'base_currency': 'USD',
                'date': currency_data['date'],
                'extracted_at': datetime.now()
            })
        
        df = pd.DataFrame(rates_list)
        
        # Load to BigQuery with explicit credentials
        credentials = service_account.Credentials.from_service_account_file(
            '/opt/airflow/config/gcp-key.json'
        )
        client = bigquery.Client(credentials=credentials)
        table_id = "etl-project-465015.raw_data.currency_rates"
        
        job = client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        ))
        job.result()
        
        print(f"✅ Loaded {len(rates_list)} currency rates to BigQuery")
        return len(rates_list)
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Currency API call failed: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        raise

def create_staging_products():
    """Create staging layer with data cleaning"""
    print("🧹 Creating staging products with data cleaning...")
    
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/config/gcp-key.json'
    )
    client = bigquery.Client(credentials=credentials)
    
    query = """
    CREATE OR REPLACE TABLE `etl-project-465015.staging.products_clean` AS
    SELECT 
        id,
        TRIM(title) as title_clean,
        LOWER(category) as category_normalized,
        ROUND(price, 2) as price_rounded,
        CASE 
            WHEN rating.rate > 5 THEN 5 
            WHEN rating.rate < 0 THEN 0 
            ELSE rating.rate 
        END as rating_capped,
        rating.count as rating_count,
        description,
        image,
        extracted_at
    FROM `etl-project-465015.raw_data.products`
    WHERE price > 0 
        AND title IS NOT NULL 
        AND category IS NOT NULL
        AND rating.rate IS NOT NULL
    """
    
    job = client.query(query)
    result = job.result()
    
    # Count rows for quality check
    count_query = "SELECT COUNT(*) as row_count FROM `etl-project-465015.staging.products_clean`"
    count_result = client.query(count_query).to_dataframe()
    row_count = count_result['row_count'].iloc[0]
    
    if row_count == 0:
        raise ValueError("No rows in staging table after cleaning!")
    
    print(f"✅ Staging table created with {row_count} clean products")
    return row_count

def create_analytics_mart():
    """Create analytics table from staging"""
    print("📊 Creating analytics mart...")
    
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/config/gcp-key.json'
    )
    client = bigquery.Client(credentials=credentials)
    
    query = """
    CREATE OR REPLACE TABLE `etl-project-465015.marts.product_analytics` AS
    SELECT 
        id,
        title_clean as title,
        category_normalized as category,
        price_rounded as price,
        rating_capped as rating,
        rating_count,
        CURRENT_DATE() as load_date
    FROM `etl-project-465015.staging.products_clean`
    ORDER BY price_rounded DESC
    """
    
    job = client.query(query)
    job.result()
    
    # Quality check
    count_query = "SELECT COUNT(*) as row_count FROM `etl-project-465015.marts.product_analytics`"
    count_result = client.query(count_query).to_dataframe()
    row_count = count_result['row_count'].iloc[0]
    
    if row_count == 0:
        raise ValueError("No rows in analytics mart!")
    
    print(f"✅ Analytics mart created with {row_count} products")
    return row_count

def run_data_quality_checks():
    """Final data quality validation"""
    print("🔍 Running final data quality checks...")
    
    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/config/gcp-key.json'
    )
    client = bigquery.Client(credentials=credentials)
    
    checks = [
        {
            'name': 'Products count',
            'query': 'SELECT COUNT(*) as count FROM `etl-project-465015.marts.product_analytics`',
            'min_value': 15
        },
        {
            'name': 'Average price reasonableness',
            'query': 'SELECT AVG(price) as avg_price FROM `etl-project-465015.marts.product_analytics`',
            'min_value': 10,
            'max_value': 1000
        },
        {
            'name': 'Rating validity',
            'query': 'SELECT MIN(rating) as min_rating, MAX(rating) as max_rating FROM `etl-project-465015.marts.product_analytics`',
            'min_rating': 0,
            'max_rating': 5
        }
    ]
    
    for check in checks:
        result = client.query(check['query']).to_dataframe()
        
        if 'min_value' in check:
            value = result.iloc[0, 0]
            if value < check['min_value']:
                raise ValueError(f"Quality check failed: {check['name']} = {value}, expected >= {check['min_value']}")
            if 'max_value' in check and value > check['max_value']:
                raise ValueError(f"Quality check failed: {check['name']} = {value}, expected <= {check['max_value']}")
            print(f"✅ {check['name']}: {value}")
        
        if 'min_rating' in check:
            min_rating = result['min_rating'].iloc[0]
            max_rating = result['max_rating'].iloc[0]
            if min_rating < 0 or max_rating > 5:
                raise ValueError(f"Rating out of bounds: min={min_rating}, max={max_rating}")
            print(f"✅ Ratings valid: {min_rating} - {max_rating}")
    
    print("✅ All quality checks passed!")
    return True

# DAG definition
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,  # Increased retries
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Enhanced ETL pipeline with quality checks',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['etl', 'bigquery', 'enhanced'],
)

# Tasks
extract_products_task = PythonOperator(
    task_id='extract_load_products',
    python_callable=extract_and_load_products,
    dag=dag,
)

extract_currency_task = PythonOperator(
    task_id='extract_load_currency',
    python_callable=extract_and_load_currency,
    dag=dag,
)

staging_task = PythonOperator(
    task_id='create_staging_products',
    python_callable=create_staging_products,
    dag=dag,
)

mart_task = PythonOperator(
    task_id='create_analytics_mart',
    python_callable=create_analytics_mart,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Dependencies - proper data pipeline flow
[extract_products_task, extract_currency_task] >> staging_task >> mart_task >> quality_check_task