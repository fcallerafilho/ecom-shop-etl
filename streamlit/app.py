import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(
    page_title="E-commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize BigQuery client
@st.cache_resource
def init_connection():
    credentials = service_account.Credentials.from_service_account_file(
        '/app/config/gcp-key.json'
    )
    return bigquery.Client(credentials=credentials)

client = init_connection()

# Cache data queries
@st.cache_data(ttl=600)
def load_product_data():
    query = """
    SELECT * FROM `etl-project-465015.marts.product_analytics`
    ORDER BY price DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def load_currency_data():
    query = """
    SELECT currency, rate 
    FROM `etl-project-465015.raw_data.currency_rates`
    WHERE currency IN ('EUR', 'GBP', 'JPY', 'CAD', 'AUD')
    ORDER BY rate DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def load_category_stats():
    query = """
    SELECT 
        category,
        COUNT(*) as product_count,
        ROUND(AVG(price), 2) as avg_price,
        ROUND(MIN(price), 2) as min_price,
        ROUND(MAX(price), 2) as max_price,
        ROUND(AVG(rating), 1) as avg_rating
    FROM `etl-project-465015.marts.product_analytics`
    GROUP BY category
    ORDER BY product_count DESC
    """
    return client.query(query).to_dataframe()

# Sidebar
st.sidebar.title("🔧 Controls")

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Last updated timestamp
st.sidebar.write(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.sidebar.markdown("---")

# Dashboard
st.title("🛍️ E-commerce Analytics Dashboard")
st.markdown("---")

# Load data
df_products = load_product_data()
df_currency = load_currency_data()
df_category_stats = load_category_stats()

# Filter controls
st.subheader("🎛️ Filters")
col1, col2, col3 = st.columns(3)

with col1:
    selected_category = st.selectbox(
        "Filter by category:", 
        ["All"] + list(df_products['category'].unique())
    )

with col2:
    price_range = st.slider(
        "Price range:",
        min_value=float(df_products['price'].min()),
        max_value=float(df_products['price'].max()),
        value=(float(df_products['price'].min()), float(df_products['price'].max())),
        step=1.0
    )

with col3:
    min_rating = st.slider(
        "Minimum rating:",
        min_value=0.0,
        max_value=5.0,
        value=0.0,
        step=0.1
    )

# Apply filters
filtered_df = df_products.copy()

if selected_category != "All":
    filtered_df = filtered_df[filtered_df['category'] == selected_category]

filtered_df = filtered_df[
    (filtered_df['price'] >= price_range[0]) & 
    (filtered_df['price'] <= price_range[1]) &
    (filtered_df['rating'] >= min_rating)
]

st.markdown("---")

# Metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Products Found", 
        len(filtered_df),
        delta=len(filtered_df) - len(df_products) if selected_category != "All" else None
    )

with col2:
    avg_price = filtered_df['price'].mean() if len(filtered_df) > 0 else 0
    st.metric("Average Price", f"${avg_price:.2f}")

with col3:
    avg_rating = filtered_df['rating'].mean() if len(filtered_df) > 0 else 0
    st.metric("Average Rating", f"{avg_rating:.1f}/5")

with col4:
    total_value = filtered_df['price'].sum() if len(filtered_df) > 0 else 0
    st.metric("Total Value", f"${total_value:.2f}")

st.markdown("---")

# Category Statistics
st.subheader("📊 Category Statistics")
st.dataframe(
    df_category_stats,
    use_container_width=True,
    column_config={
        "category": "Category",
        "product_count": st.column_config.NumberColumn("Products", format="%d"),
        "avg_price": st.column_config.NumberColumn("Avg Price", format="$%.2f"),
        "min_price": st.column_config.NumberColumn("Min Price", format="$%.2f"),
        "max_price": st.column_config.NumberColumn("Max Price", format="$%.2f"),
        "avg_rating": st.column_config.NumberColumn("Avg Rating", format="%.1f ⭐")
    }
)

st.markdown("---")

# Charts row
col1, col2 = st.columns(2)

with col1:
    st.subheader("📦 Products by Category")
    if len(filtered_df) > 0:
        category_counts = filtered_df['category'].value_counts()
        fig = px.pie(
            values=category_counts.values, 
            names=category_counts.index,
            title="Product Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("No products match the current filters.")

with col2:
    st.subheader("💰 Price Distribution")
    if len(filtered_df) > 0:
        fig = px.histogram(
            filtered_df, 
            x='price', 
            nbins=20,
            title="Product Price Range"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("No products match the current filters.")

# Rating vs Price scatter plot
st.subheader("⭐ Rating vs Price Analysis")
if len(filtered_df) > 0:
    fig = px.scatter(
        filtered_df,
        x='price',
        y='rating',
        color='category',
        size=filtered_df['rating_count'].astype(float),  # Fix: convert to float
        hover_data=['title'],
        title="Product Rating vs Price (size = rating count)"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.write("No products match the current filters.")

# Currency rates
st.subheader("💱 Major Currency Rates (vs USD)")
fig = px.bar(
    df_currency, 
    x='currency', 
    y='rate',
    title="Exchange Rates",
    color='rate',
    color_continuous_scale='viridis'
)
st.plotly_chart(fig, use_container_width=True)

# Product table
st.subheader("🛍️ Product Details")
if len(filtered_df) > 0:
    st.dataframe(
        filtered_df[['title', 'category', 'price', 'rating', 'rating_count']], 
        use_container_width=True,
        column_config={
            "title": "Product Title",
            "category": "Category", 
            "price": st.column_config.NumberColumn("Price", format="$%.2f"),
            "rating": st.column_config.NumberColumn("Rating", format="%.1f ⭐"),
            "rating_count": "Reviews"
        }
    )
else:
    st.write("No products match the current filters.")

# Footer
st.markdown("---")
st.markdown("**Data Pipeline:** APIs → Airflow → BigQuery → Streamlit")