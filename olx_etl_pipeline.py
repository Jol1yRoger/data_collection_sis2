import time
import random
import re
import pandas as pd
import sqlite3
from playwright.sync_api import sync_playwright

DB_PATH = 'olx_data.db'  
RAW_CSV_PATH = '/tmp/olx_raw_data.csv'
def scrape_olx_data(**kwargs):
    base_url = "https://www.olx.kz/elektronika/telefony-i-aksesuary/"
    all_products = []
    target_count = 125
    current_page = 1
    
    print("--- Start Scraping ---")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, 
            args=[
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled' 
            ]
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080} 
        )
        page = context.new_page()

        while len(all_products) < target_count:
            url = f"{base_url}?page={current_page}"
            try:
                print(f"Parsing: {url}")
                page.goto(url, timeout=60000)
                page.wait_for_selector('div[data-cy="l-card"]', timeout=20000)
                
                cards = page.locator('div[data-cy="l-card"]').all()
                print(f"Найдено карточек на странице: {len(cards)}")
                
                if not cards:
                    print("Карточки не найдены! Возможно, капча или бан.")
                    break
                
                for card in cards:
                    if not card.is_visible():
                        continue
                        
                    item = {}
                    link_el = card.locator('a').first
                    title_el = link_el.locator('h4')
                    price_el = card.locator('[data-testid="ad-price"]')
                    loc_el = card.locator('[data-testid="location-date"]')

                    item['title'] = title_el.inner_text() if title_el.count() else "No Title"
                    
                    href = link_el.get_attribute('href')
                    if href:
                        item['link'] = f"https://www.olx.kz{href}" if href.startswith('/') else href
                    else:
                        item['link'] = "No Link"

                    item['raw_price'] = price_el.inner_text() if price_el.count() else "0"
                    item['raw_location_date'] = loc_el.inner_text() if loc_el.count() else "Unknown"

                    all_products.append(item)
                
                print(f"Collected total: {len(all_products)}")
                current_page += 1
                time.sleep(random.uniform(3, 6)) 

            except Exception as e:
                print(f"Error on page {current_page}: {e}")
                page.screenshot(path=f"/tmp/error_page_{current_page}.png")
                break

        browser.close()

    if len(all_products) == 0:
        raise ValueError("CRITICAL: Не удалось собрать ни одного товара! Проверь логи или скриншоты.")

    df = pd.DataFrame(all_products)
    df.to_csv(RAW_CSV_PATH, index=False)
    print(f"Raw data saved to {RAW_CSV_PATH}. Total rows: {len(df)}")

def process_and_save_data(**kwargs):
    """
    TRANSFORM & LOAD: Очистка данных и сохранение в SQLite
    """
    print("--- Start Processing ---")
    
    try:
        df = pd.read_csv(RAW_CSV_PATH)
    except FileNotFoundError:
        print("Raw data file not found!")
        return

    initial_count = len(df)
    df = df.drop_duplicates(subset=['link'])
    print(f"Duplicates removed: {initial_count - len(df)}")

    df = df.dropna(subset=['title', 'raw_price'])
    df['raw_location_date'] = df['raw_location_date'].fillna("Unknown - Unknown")

    df['title'] = df['title'].astype(str).str.strip().str.title() 
    
    def split_loc_date(val):
        parts = val.split(' - ')
        if len(parts) >= 2:
            return parts[0].strip(), parts[-1].strip()
        return val, "Unknown"

    df[['location', 'date_posted']] = df['raw_location_date'].astype(str).apply(lambda x: pd.Series(split_loc_date(x)))

    def clean_price(price_str):
        digits_only = re.sub(r'[^\d]', '', str(price_str))
        if digits_only:
            return int(digits_only)
        return 0
    df['price'] = df['raw_price'].apply(clean_price)

    df_clean = df[['title', 'price', 'location', 'date_posted', 'link']].copy()

    print("Data processed successfully.")
    print(df_clean.head())

    
    print(f"Saving to SQLite database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    
    df_clean.to_sql('olx_items', conn, if_exists='append', index=False)
    
    conn.close()
    print("Data successfully loaded into SQLite3.")








