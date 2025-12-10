import time
import random
import csv
import sqlite3
import sys
import concurrent.futures
import shutil
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- Configuration ----------
DB_FILE = "products.db"      # SQLite database file
OUTPUT_CSV = "products_export.csv" # Final export file
NUM_WORKERS = 4              # Number of parallel browser windows
HEADLESS = False             # set True if you don't want browser window
MAX_WAIT = 5                # seconds, implicit wait
MIN_DELAY = 2                # polite delay lower bound (seconds)
MAX_DELAY = 4                # polite delay upper bound (seconds)
TIMEOUT_NAV = 20             # page load timeout (increased from 5 to 20)

# List of User-Agents to rotate through
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]
# -----------------------------------

def setup_database():
    """Creates the database and table if they don't exist."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            link TEXT PRIMARY KEY,
            product_name TEXT,
            price TEXT,
            discount TEXT,
            error TEXT,
            scraped_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print(f"Database '{DB_FILE}' is ready.")

def load_urls_from_db():
    """Loads URLs from the database that haven't been scraped yet."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Select links that have not been scraped or had a previous error
    cursor.execute("SELECT link FROM products WHERE product_name IS NULL OR error IS NOT NULL")
    urls = [row[0] for row in cursor.fetchall()]
    conn.close()
    if not urls:
        print(f"No new URLs to process in '{DB_FILE}'. Add links to the 'products' table.")
        sys.exit(1)
    return urls

def update_db_record(result):
    """Writes a scraped result back to the database."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE products
        SET product_name = ?, price = ?, discount = ?, error = ?, scraped_at = CURRENT_TIMESTAMP
        WHERE link = ?
    """, (result['product_name'], result['Price'], result['discount'], result['error'], result['link']))
    conn.commit()
    conn.close()

def setup_driver(user_agent):
    chrome_options = Options()
    chrome_options.add_argument("--incognito")
    if HEADLESS:
        chrome_options.add_argument("--headless=new")  # headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Set a realistic user-agent to reduce bot detection
    chrome_options.add_argument(f"--user-agent={user_agent}")
    # Optional: disable images for speed (uncomment if desired)
    # prefs = {"profile.managed_default_content_settings.images": 2}
    # chrome_options.add_experimental_option("prefs", prefs)

    try:
        # webdriver-manager can sometimes return an incorrect path to a notice file.
        # We will get the path and then find the actual executable.
        installed_path_str = ChromeDriverManager().install()
        print(f"webdriver-manager installed path: {installed_path_str}")

        # Find the actual chromedriver.exe
        driver_path_obj = Path(installed_path_str)
        # The actual executable should be in the same directory or a parent directory.
        search_dir = driver_path_obj.parent
        driver_exe_path = search_dir / "chromedriver.exe"

        if not driver_exe_path.is_file():
            raise FileNotFoundError(f"Could not find chromedriver.exe in {search_dir}")
        driver_path = str(driver_exe_path)
        print(f"Found and using ChromeDriver at: {driver_path}")
    except Exception as e:
        print(f"Error installing or finding ChromeDriver: {e}")
        sys.exit(1)
    service = ChromeService(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(TIMEOUT_NAV)
    driver.implicitly_wait(MAX_WAIT)
    return driver

def try_find(driver, by, value):
    try:
        el = driver.find_element(by, value)
        text = el.text.strip()
        if not text:
            # sometimes price is inside input/value or innerHTML; try get_attribute
            text = el.get_attribute("innerText") or el.get_attribute("textContent") or ""
            text = text.strip()
        return text
    except NoSuchElementException:
        return ""

def extract_title(driver):
    # Common title selectors
    selectors = [
        (By.ID, "productTitle"),
        (By.CSS_SELECTOR, "#titleSection #productTitle"),
        (By.CSS_SELECTOR, "span#title"),
        (By.CSS_SELECTOR, "h1.a-size-large.a-spacing-none"),
        (By.CSS_SELECTOR, "h1"),
    ]
    for by, sel in selectors:
        t = try_find(driver, by, sel)
        if t:
            return t
    return ""

def extract_price(driver):
    # Amazon price appears in many selectors depending on region, deals, etc.
    price_selectors = [
        (By.ID, "priceblock_ourprice"),
        (By.ID, "priceblock_dealprice"),
        (By.ID, "priceblock_saleprice"),
        (By.ID, "tp_price_block_total_price_ww"),
        (By.CSS_SELECTOR, "span.a-price > span.a-offscreen"),   # common modern selector
        (By.CSS_SELECTOR, "span#price_inside_buybox"),
        (By.CSS_SELECTOR, "div#corePriceDisplay_desktop_feature_div span.a-offscreen"),
        (By.CSS_SELECTOR, "div.a-section.a-spacing-none.aok-align-center span.a-price-whole"),  # numeric part
        (By.CSS_SELECTOR, "span.offer-price"),
        (By.CSS_SELECTOR, "span.priceToPay > span"),
    ]
    # Try each; if selector returns multiple characters that look like price, return
    for by, sel in price_selectors:
        txt = try_find(driver, by, sel)
        if txt:
            # normalize whitespace
            txt = " ".join(txt.split())
            # quick sanity: price should contain a digit and possibly currency symbol
            if any(ch.isdigit() for ch in txt):
                return txt
    # Fallback: search for any span with class 'a-offscreen' (sometimes multiple)
    try:
        offscreens = driver.find_elements(By.CSS_SELECTOR, "span.a-offscreen")
        for o in offscreens:
            txt = (o.get_attribute("innerText") or o.text or "").strip()
            if txt and any(ch.isdigit() for ch in txt):
                return txt
    except Exception:
        pass
    return ""

def extract_discount(driver):
    # Amazon discount selectors can vary. Look for savings percentage or amount.
    discount_selectors = [
        (By.CSS_SELECTOR, "span.savingsPercentage"), # e.g., "-30%"
        (By.CSS_SELECTOR, "td.a-span12.a-color-price.a-size-base"), # e.g., "You Save: $50.00 (25%)"
        (By.CSS_SELECTOR, "div#corePriceDisplay_desktop_feature_div tr:nth-child(3) td.a-span12"),
    ]
    for by, sel in discount_selectors:
        txt = try_find(driver, by, sel)
        if txt:
            # Normalize whitespace and return if found
            return " ".join(txt.split())
    return ""

def scrape_one(driver, url):
    result = {"link": url, "product_name": "", "Price": "", "discount": "", "error": ""}
    try:
        driver.get(url)
    except (TimeoutException, WebDriverException) as e:
        # try once more after short wait
        try:
            time.sleep(3)
            driver.get(url)
        except Exception as e2:
            result["error"] = f"Page load failed on retry: {e2} (original error: {e})"
            return result

    # random polite delay to allow dynamic content to load
    time.sleep(random.uniform(1.0, 2.0))

    # Some pages show a cookie or location overlay; try closing common overlays
    for close_selector in [
        ("css", "button#sp-cc-accept"),        # cookie banner
        ("css", "input#glowDoneButton"),       # location popup
        ("css", "button[name='accept']"),
        ("css", "button#nav-main .nav-left"),  # generic
    ]:
        kind, sel = close_selector
        try:
            if kind == "css":
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    try:
                        el.click()
                        time.sleep(0.3)
                    except Exception:
                        pass
        except Exception:
            pass

    # Attempt title and price extraction
    title = extract_title(driver)
    price = extract_price(driver)
    discount = extract_discount(driver)

    # Extra attempt: some pages show price inside scripts or meta tags
    if not price:
        try:
            # meta og:price:amount or product:price:amount
            metas = driver.find_elements(By.XPATH, "//meta[contains(@property,'price') or contains(@name,'price')]")
            for m in metas:
                val = (m.get_attribute("content") or "").strip()
                if val and any(ch.isdigit() for ch in val):
                    price = val
                    break
        except Exception:
            pass

    # If still empty, try small delay and re-check (for lazy-loaded price)
    if not price:
        time.sleep(1.0)
        price = extract_price(driver)

    result["product_name"] = title
    result["Price"] = price
    result["discount"] = discount
    if not title and not price:
        result["error"] = "Could not find title or price. Page layout may be unexpected."
    return result

def clear_webdriver_cache():
    """
    Clears the webdriver-manager cache to fix potential corruption issues.
    This can resolve OSError: [WinError 193] %1 is not a valid Win32 application.
    """
    try:
        cache_path = Path.home() / ".wdm"
        if cache_path.exists():
            print("Clearing webdriver-manager cache...")
            shutil.rmtree(cache_path)
    except Exception as e:
        print(f"Warning: Could not clear webdriver-manager cache: {e}")

def process_url(url, worker_id):
    """
    Handles the entire process for a single URL within a worker thread.
    Initializes driver, scrapes, and quits driver.
    """
    print(f"[Worker-{worker_id}] Starting URL: {url[:80]}")
    user_agent = random.choice(USER_AGENTS)
    driver = setup_driver(user_agent)
    try:
        result = scrape_one(driver, url)
        update_db_record(result)
        print(f"  -> [Worker-{worker_id}] Name: {result['product_name'][:70]}")
        print(f"  -> [Worker-{worker_id}] Price: {result['Price']}")
        print(f"  -> [Worker-{worker_id}] Discount: {result['discount']}")
        if result.get("error"):
            print(f"  -> [Worker-{worker_id}] Error: {result['error']}")
    except Exception as e:
        print(f"  -> [Worker-{worker_id}] CRITICAL ERROR processing {url}: {e}")
        update_db_record({"link": url, "product_name": "", "Price": "", "discount": "", "error": str(e)})
    finally:
        driver.quit()
        # Polite delay before this worker picks up a new task
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return url

def export_db_to_csv():
    """Reads all data from the database and exports it to a CSV file."""
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM products", conn)
    conn.close()
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    print(f"\nSuccessfully exported {len(df)} records from database to '{OUTPUT_CSV}'.")

def main():
    clear_webdriver_cache()
    setup_database()
    urls = load_urls_from_db()
    print(f"Found {len(urls)} unique URLs to process.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Assign a worker ID to each task for better logging
        future_to_url = {executor.submit(process_url, url, i % NUM_WORKERS + 1): url for i, url in enumerate(urls)}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                print(f'{url} generated an exception: {exc}')

    print("\nScraping complete. Exporting results to CSV...")
    export_db_to_csv()

if __name__ == "__main__":
    main()