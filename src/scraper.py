from bs4 import BeautifulSoup
import requests
from datetime import datetime
import time
from urllib.parse import urljoin

BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"

def fetch_html(url):
    """
    Fetches HTML content from a URL with basic error handling
   
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Error messages if there is an erro while reaching the url
        return response.text
    except requests.RequestException as e: 
        print(f"Error while Fetching: {url}: {e}")
        return None
    

def parse_books(html_content):

    soup = BeautifulSoup(html_content, 'html.parser')
    books = []

    for article in soup.find_all('article', class_='product_pod'):
        try:
            img_tag = article.find('img')
            image_url = urljoin("https://books.toscrape.com/catalogue/", img_tag['src'])
            
            title = article.find('h3').find('a')['title']
            price = article.find('p', class_='price_color').text
            star_reviews = article.find('p', class_='star-rating')['class']
            rating = [x for x in star_reviews if x != 'star-rating'][0]
            
            in_stock = article.find('p', class_='instock availability')
            availability_text = in_stock.text.strip() if in_stock else 'N/A'

            books.append({
                'img': image_url,
                'title': title,
                'price': price,
                'rating': rating,
                'availability': availability_text,
                'scrapped_at': datetime.now().isoformat()
            })
        except AttributeError as e:
            print(f"⚠️ Skipping book due to missing element: {e}")
            continue
    return books

def run_scraper(pages_to_scrape =5):
    """
    Main function to orchestrate the scraping
    """
    print(f"🚀 Starting scraper for {pages_to_scrape} pages...")

    all_data = []
    
    for page in range(1, pages_to_scrape+1):
    
        url = BASE_URL.format(page)
    
        html = fetch_html(url)
        
        
        if html:
            
            print(f"✅ Success! Received {len(html)} bytes of HTML from {url}") 
            
            data = parse_books(html)
            
            print(f"📦 Parser found {len(data)} items on this page.") 

            all_data.extend(data)
        else:
            print(f"❌ Failed to retrieve content for page {page}. Moving to next.")
        
    time.sleep(1)

    print(f'Scrapping complete. Collected {len(all_data)} items.')
    return all_data


if __name__ == '__main__':
    data = run_scraper(pages_to_scrape=5)
    