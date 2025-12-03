import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import os
from datetime import datetime


DB_PATH = os.path.join(os.getcwd(), 'data', 'books_data.db') 
DATABASE_URL = f'sqlite:///{DB_PATH}'

# Streamlit page settings
st.set_page_config(
    page_title="E-commerce Market Analyzer",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Data Loading Function (C-R-U-D) ---
@st.cache_data(ttl=600) # Cache the data for 10 minutes (600 seconds)
def load_data_from_db():
    """Connects to the database and fetches all books data."""
    if not os.path.exists(DB_PATH):
        st.error("Database file not found! Please run python -m src.etl first.")
        return pd.DataFrame()

    engine = create_engine(DATABASE_URL)
    
    try:
        # Fetch all data from the books_data table
        query = "SELECT * FROM books_data"
        df = pd.read_sql(query, engine)
        
        # Convert timestamp field back to datetime object for sorting/plotting
        df['scraped_timestamp'] = pd.to_datetime(df['scraped_timestamp'])
        
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()


def main():
    st.title("📚 E-commerce Book Market Analyzer")
    st.markdown("---")

    df = load_data_from_db()

    if df.empty:
        st.warning("No data available to display. Run the ETL pipeline to populate the database.")
        return

    # 1. SIDEBAR FILTERS
    with st.sidebar:
        st.header("Filters")
        
        # Filter by Star Rating
        unique_ratings = sorted(df['rating'].unique())
        selected_ratings = st.multiselect(
            'Filter by Star Rating (min)',
            options=unique_ratings,
            default=unique_ratings
        )
        
        # Filter by Availability
        unique_availability = df['availability'].unique()
        selected_availability = st.multiselect(
            'Filter by Availability',
            options=unique_availability,
            default=unique_availability
        )
        
        # Apply filters
        df_filtered = df[df['rating'].isin(selected_ratings) & 
                         df['availability'].isin(selected_availability)]

    # 2. KEY METRICS (KPIs)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_books = len(df_filtered)
        st.metric(label="Total Books Analyzed", value=f"{total_books:,}")
    
    with col2:
        avg_price = df_filtered['price'].mean()
        st.metric(label="Average Book Price", value=f"£{avg_price:.2f}")

    with col3:
        # Find the timestamp of the latest scrape
        last_scraped = df['scraped_timestamp'].max()
        st.metric(label="Last Data Update", value=last_scraped.strftime("%Y-%m-%d %H:%M"))

    st.markdown("---")

    # 3. VISUALIZATION (Plotly Chart)
    st.subheader("Book Price Distribution by Rating")
    
    # Use Plotly to create an interactive box plot
    fig = px.box(
        df_filtered,
        x="rating",
        y="price",
        color="rating",
        title="Price Range and Outliers Grouped by Star Rating",
        labels={"rating": "Star Rating", "price": "Price (£)"},
        # Ensure x-axis is treated as a category
        category_orders={"rating": sorted(df_filtered['rating'].unique())}
    )
    fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray': sorted(df_filtered['rating'].unique())})
    st.plotly_chart(fig, use_container_width=True)

    # 4. RAW DATA TABLE
    st.subheader("Raw Data Table")
    # Format the timestamp for display only
    df_display = df_filtered.copy()
    df_display['scraped_timestamp'] = df_display['scraped_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    st.dataframe(df_display, use_container_width=True)


if __name__ == '__main__':
    main()