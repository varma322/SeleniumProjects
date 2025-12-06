"""
amazon_scrape_to_excel.py

Reads product URLs from 'links.txt' (one URL per line) or 'links.csv' (column named 'url'),
scrapes product title and price from each Amazon product page using Selenium,
and writes results to 'products.xlsx'.

Notes:
- Amazon changes HTML over time; this script tries multiple selectors commonly seen.
- Use responsibly. Amazon may block aggressive scraping; don't hammer their servers.
"""

import time
import random
import csv
import sys
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- Configuration ----------
INPUT_TXT = "links.txt"      # one URL per line
INPUT_CSV = "links.csv"      # optional: column named 'url'
OUTPUT_XLSX = "products.xlsx"
HEADLESS = False             # set True if you don't want browser window
MAX_WAIT = 5                # seconds, implicit wait
MIN_DELAY = 2                # polite delay lower bound (seconds)
MAX_DELAY = 4                # polite delay upper bound (seconds)
TIMEOUT_NAV = 20             # page load timeout (increased from 5 to 20)
# -----------------------------------

def load_urls():
    urls = []
    if Path(INPUT_TXT).is_file():
        with open(INPUT_TXT, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u:
                    urls.append(u)
    if Path(INPUT_CSV).is_file():
        df = pd.read_csv(INPUT_CSV)
        if 'url' in df.columns:
            urls.extend(df['url'].dropna().astype(str).str.strip().tolist())
        else:
            print(f"Warning: {INPUT_CSV} exists but has no 'url' column. Skipping it.")
    if not urls:
        print("No URLs found. Please create 'links.txt' (one URL per line) or 'links.csv' with a 'url' column.")
        sys.exit(1)
    # dedupe while preserving order
    seen = set()
    final = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            final.append(u)
    return final

def setup_driver():
    chrome_options = Options()
    if HEADLESS:
        chrome_options.add_argument("--headless=new")  # headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Set a realistic user-agent to reduce bot detection
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    # Optional: disable images for speed (uncomment if desired)
    # prefs = {"profile.managed_default_content_settings.images": 2}
    # chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
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
    # Selectors for discount percentage
    discount_selectors = [
        (By.CSS_SELECTOR, "span.savingsPercentage"),
        (By.CSS_SELECTOR, ".reinventPriceSavingsPercentageMargin"),
        (By.CSS_SELECTOR, "td.a-span12.a-color-price.a-size-base span[aria-hidden='true']"),
    ]
    for by, sel in discount_selectors:
        txt = try_find(driver, by, sel)
        if txt and ('%' in txt or any(ch.isdigit() for ch in txt)):
            # Clean up text like "(-15%)" to "15%"
            txt = txt.replace('-', '').replace('(', '').replace(')', '').strip()
            return txt
    return ""

def scrape_one(driver, url):
    result = {"url": url, "title": "", "price": "", "discount": "", "error": ""}
    try:
        driver.get(url)
    except (TimeoutException, WebDriverException) as e:
        # try once more after short wait
        try:
            time.sleep(3)
            driver.get(url)
        except Exception as e2:
            result["error"] = f"Page load failed: {e} / {e2}"
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
    if not discount: # and for discount
        time.sleep(0.5)
        discount = extract_discount(driver)

    result["title"] = title
    result["price"] = price
    result["discount"] = discount
    if not title and not price:
        result["error"] = "Could not find title or price. Page layout may be unexpected."
    return result

def main():
    urls = load_urls()
    print(f"Found {len(urls)} unique URLs to process.")
    driver = setup_driver()

    rows = []
    try:
        for idx, url in enumerate(urls, start=1):
            print(f"[{idx}/{len(urls)}] Processing: {url}")
            row = scrape_one(driver, url)
            print(f"  -> title: {row['title'][:80] + ('...' if len(row['title'])>80 else '')}")
            print(f"  -> price: {row['price']}")
            print(f"  -> discount: {row['discount']}")
            if row.get("error"):
                print(f"  -> error: {row['error']}")
            rows.append(row)
            # polite randomized delay between requests
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    finally:
        driver.quit()

    # Save to Excel (and CSV)
    df = pd.DataFrame(rows)
    # Move columns order
    cols = ["url", "title", "price", "discount", "error"]
    df = df[[c for c in cols if c in df.columns]]
    df.to_excel(OUTPUT_XLSX, index=False)
    df.to_csv("products.csv", index=False)
    print(f"Saved {len(df)} rows to {OUTPUT_XLSX} and products.csv")

if __name__ == "__main__":
    main()