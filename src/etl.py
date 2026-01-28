from .scraper  import run_scraper
from sqlalchemy import create_engine, text
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'books_data.db')

RATING_MAP = {
    'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5, 
    'Zero': 0, 'N/A': 0
}


def clean_data(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data)

    if(df.empty):
        print('⚠️ DataFrame is empty, No Data to clean')
        return df
    
    """
    Sample output of data 
        ('img', 'https://books.toscrape.com/catalogue/media/cache/2c/da/2cdad67c44b002e7ead0cc35693c0e8b.jpg')
        ('title', 'A Light in the Attic')
        ('price', 'Â£51.77')
        ('rating', 'Three')
        ('availability', 'In stock')
        ('scrapped_at', '2025-12-02T22:06:25.282749')
    """
    df['price'] = df['price'].astype(str).str.extract(r'(\d+\.\d+)').astype(float)

    df['rating'] = df['rating'].map(RATING_MAP).fillna(0).astype(int)

    df['scrapped_at'] = pd.to_datetime(df['scrapped_at'])

    df_cleaned = df.rename(columns = {'scrapped_at': 'scraped_timestamp'})

    final_cols = ['img', 'title', 'price', 'rating', 'availability',
       'scraped_timestamp']
    
    return df_cleaned[final_cols]


## Data is cleaned now its time to Load it to SQLlite Database

def load_data(df: pd.DataFrame):
    """Loads the DataFrame into the SQLite database."""
    
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Create the database engine
    engine = create_engine(f'sqlite:///{DB_PATH}')
    
    print(f"\n💾 Loading {len(df)} records into the database...")
    

    df.to_sql('books_data', engine, if_exists='replace', index=False)
    

    with engine.connect() as connection:
       
        total_rows = connection.execute(text("SELECT COUNT(*) FROM books_data")).scalar()
        print(f"✅ Data loaded successfully. Total rows in table: {total_rows}")
        
    print(f"Database saved to: {DB_PATH}")

def run_etl():
    print("----Starting ETL process-------")

    raw_data = run_scraper(pages_to_scrape=5)

    if not raw_data:
        print('ETL Aborted: No data returned from scraper')
        return 
    
    clean_df = clean_data(raw_data)

    if clean_df.empty:
        return
    
    load_data(clean_df)

    print('-------ETL Process Finished-------------')


if __name__ == '__main__':
    run_etl()


