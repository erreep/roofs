#!/usr/bin/env python3
import time, random

# before you do any driver.get(...)
# This initial sleep helps to vary start times if you run multiple instances or on a schedule,
# making it less predictable.
time.sleep(random.uniform(10, 30))   # wait 10–30 seconds randomly

import os
import re
import time
import tempfile
import psycopg2
import asyncio
from datetime import datetime, timedelta, timezone # Added timedelta and timezone

from dotenv import load_dotenv
load_dotenv()  # loads DATABASE_URL, TELEGRAM_TOKEN, and PROXY variables from .env

from telegram import Bot
from seleniumwire import webdriver as sw_webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)

# --- CONFIG & DB SETUP ---
DB_URL         = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# PROXY CONFIGURATION - Read from .env
# PROXY_LIST_STR: Comma-separated list of proxies in "host:port:user:pass" format
PROXY_LIST_STR = os.getenv("PROXY_LIST")
# PROXY_USERNAME and PROXY_PASSWORD are now parsed from PROXY_LIST_STR directly in get_driver
PROXY_USERNAME = None 
PROXY_PASSWORD = None 

# --- Environment Variable Checks ---
if not DB_URL:
    raise RuntimeError("DATABASE_URL must be set in .env")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN must be set in .env")

# Create a single asyncio loop for the script
# This is used for async Telegram bot operations
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# DB connection & ensure tables exist
try:
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # --- Database Migration/Schema Update for 'last_seen' column ---
    # This block ensures the 'last_seen' column exists and replaces 'scraped_at' if present.
    # It handles both new databases and existing ones that might have 'scraped_at'.
    cur.execute("""
    DO $$
    BEGIN
        -- Check if 'scraped_at' column exists in 'listings' table
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'listings' AND column_name = 'scraped_at'
        ) THEN
            -- If 'last_seen' does not exist, rename 'scraped_at' to 'last_seen'
            IF NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'listings' AND column_name = 'last_seen'
            ) THEN
                ALTER TABLE listings RENAME COLUMN scraped_at TO last_seen;
                RAISE NOTICE 'Renamed column scraped_at to last_seen in listings table.';
            ELSE
                -- If both exist (shouldn't happen with clean migration, but for robustness)
                -- we'll assume last_seen is preferred and scraped_at is obsolete.
                ALTER TABLE listings DROP COLUMN scraped_at;
                RAISE NOTICE 'Dropped obsolete scraped_at column as last_seen already exists.';
            END IF;
        END IF;

        -- Ensure 'last_seen' column exists, adding it if it's completely new
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = 'listings' AND column_name = 'last_seen'
        ) THEN
            ALTER TABLE listings ADD COLUMN last_seen TIMESTAMPTZ DEFAULT now();
            RAISE NOTICE 'Added new column last_seen to listings table.';
        END IF;
    END
    $$;
    """)

    # Create listings table if it doesn't exist (for fresh databases)
    # This ensures the table structure is correct if it's created for the first time.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        link       TEXT PRIMARY KEY,
        title      TEXT,
        location   TEXT,
        city       TEXT,
        rent       TEXT,
        service    TEXT,
        last_seen TIMESTAMPTZ DEFAULT now() -- Use last_seen from the start
    );
    """)
    # Create subscriptions table if it doesn't exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS subscriptions (
        chat_id       BIGINT    NOT NULL,
        city          TEXT      NOT NULL,
        subscribed_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (chat_id, city)
    );
    """)
    conn.commit() # Commit table creation and migrations
except Exception as e:
    print(f"Error connecting to database or creating tables: {e}")
    exit(1) # Exit if DB connection fails at startup


# Telegram bot client
bot = Bot(token=TELEGRAM_TOKEN)


def get_driver():
    """
    Initializes and returns a Selenium WebDriver with configured options and proxy settings.
    """
    opts = Options()
    # Run in headless mode (no visible browser UI)
    opts.add_argument("--headless=new")
    # Set window size for consistent scraping results
    opts.add_argument("--window-size=1920,1080")
    # Recommended for running Chrome in a Docker container or on a server without a display
    opts.add_argument("--no-sandbox")
    # Overcome limited resource problems, especially in containerized environments
    opts.add_argument("--disable-dev-shm-usage")
    # Disable GPU hardware acceleration (often not available in headless/server environments)
    opts.add_argument("--disable-gpu")

    # Use a fresh temporary profile each run to avoid cached data/locked-profile errors
    profile = tempfile.mkdtemp(prefix="chrome-profile-")
    opts.add_argument(f"--user-data-dir={profile}")

    # Automatically download and manage the ChromeDriver executable
    svc = Service(ChromeDriverManager().install())

    # --- PROXY IMPLEMENTATION START (using selenium-wire) ---
    seleniumwire_options = {}
    if PROXY_LIST_STR:
        # Split the comma-separated proxy list and clean up whitespace
        raw_proxy_strings = [p.strip() for p in PROXY_LIST_STR.split(',') if p.strip()]
        
        if raw_proxy_strings:
            # Randomly select one full proxy string (e.g., "host:port:user:pass")
            selected_raw_proxy = random.choice(raw_proxy_strings)
            
            # Parse the selected proxy string into its components
            parts = selected_raw_proxy.split(':')
            if len(parts) == 4:
                host, port, user, password = parts
                
                # Construct the proxy URL with embedded credentials
                proxy_url_with_auth = f"http://{user}:{password}@{host}:{port}"
                print(f"Using proxy: {host}:{port} with user '{user}' (selected from list).")

                # Assign the proxy configuration to seleniumwire_options
                seleniumwire_options['proxy'] = {
                    'http': proxy_url_with_auth,
                    'https': proxy_url_with_auth,
                    'no_proxy': 'localhost,127.0.0.1' # Exclude local traffic
                }
            else:
                print(f"WARNING: Invalid proxy string format in PROXY_LIST: {selected_raw_proxy}. Expected 'host:port:user:pass'. Skipping proxy.")
        else:
            print("WARNING: PROXY_LIST is set but contains no valid proxy entries.")
    else:
        print("INFO: PROXY_LIST is not set. Running without proxy.")
    # --- PROXY IMPLEMENTATION END ---

    # Initialize the Chrome driver using seleniumwire's webdriver.Chrome
    # This enables proxy handling through seleniumwire_options
    return sw_webdriver.Chrome(
        service=svc,
        options=opts,
        seleniumwire_options=seleniumwire_options
    )


def scrape_listings(driver, wait):
    """
    Navigates to the listings page, scrapes property details, and handles pagination.
    """
    driver.get("https://www.roofz.eu/availability")
    all_items = []

    while True:
        # Wait until property cards are present on the page
        try:
            wait.until(EC.presence_of_all_elements_located((
                By.CSS_SELECTOR, "div.property-cards__single div.property"
            )))
        except TimeoutException:
            print("No property cards found on page, or page took too long to load. Exiting scrape loop.")
            break # Exit if no cards appear

        cards = driver.find_elements(By.CSS_SELECTOR, "div.property-cards__single div.property")

        for card in cards:
            try:
                title    = card.find_element(By.CSS_SELECTOR, ".property__title").text.strip()
                loc_full = card.find_element(By.CSS_SELECTOR, ".property__location").text.strip()
                # Find city, specifically within the location span
                city     = card.find_element(
                              By.CSS_SELECTOR,
                              ".property__location span.capitalize"
                           ).text.strip()
                # Get rent and service prices
                prices   = card.find_elements(By.CSS_SELECTOR, "div.property__price .highlighted")
                rent     = prices[0].text.strip()
                service  = prices[1].text.strip()
                # Get the direct link to the listing
                link     = card.find_element(By.CSS_SELECTOR, "a.property__link") \
                                  .get_attribute("href")

                all_items.append({
                    "title":    title,
                    "location": loc_full,
                    "city":     city,
                    "rent":     rent,
                    "service":  service,
                    "link":     link
                })
            except NoSuchElementException as e:
                # If any expected element is missing in a card, skip it and print a warning
                print(f"WARNING: Skipping a card due to missing element: {e}")
                continue # Move to the next card

        # Try to click the "Next" button for pagination
        try:
            nxt = wait.until(EC.presence_of_element_located((
                By.CSS_SELECTOR, 'button[aria-label="Go to next page"]'
            )))
            # Scroll the next button into view to ensure it's clickable
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
            time.sleep(0.3) # Short pause after scroll

            # Store a reference to the first card before clicking next.
            # We'll use this to wait for the page to fully reload.
            first_card_on_current_page = cards[0] if cards else None

            try:
                nxt.click() # Attempt direct click
            except (ElementNotInteractableException, ElementClickInterceptedException):
                # Fallback to JavaScript click if direct click fails
                driver.execute_script("arguments[0].click();", nxt)
            
            # Wait for the first card of the previous page to become stale,
            # indicating the new page has loaded.
            if first_card_on_current_page:
                wait.until(EC.staleness_of(first_card_on_current_page))
            else:
                # If for some reason no cards were found on the previous page,
                # just wait a moment to ensure the page has a chance to load.
                time.sleep(2) 
        except (TimeoutException, NoSuchElementException):
            # If "Next" button is not found (Timeout) or some other element issue,
            # it means we've reached the last page or pagination is broken.
            break # Exit the pagination loop

    # Deduplicate listings by link (ensuring unique entries)
    unique = {item["link"]: item for item in all_items}
    return list(unique.values())


def notify_new_listings(new_items):
    """
    Sends Telegram notifications for new listings to subscribed chat_ids.
    """
    # Group listings by city for targeted notifications
    by_city = {}
    for apt in new_items:
        by_city.setdefault(apt["city"], []).append(apt)

    # For each city, fetch subscribers and send messages
    for city, apts in by_city.items():
        cur.execute("SELECT chat_id FROM subscriptions WHERE city = %s", (city,))
        chat_ids = [r[0] for r in cur.fetchall()]
        if not chat_ids:
            continue # No subscribers for this city

        # Construct the Telegram message text
        text = f"🏠 *Nieuwe listings in {city}:*\n\n" + "\n\n".join(
            f"[{apt['title']}]({apt['link']}) — {apt['rent']} + {apt['service']}"
            for apt in apts
        )

        # Send each message via the asyncio loop
        for chat_id in chat_ids:
            try:
                loop.run_until_complete(
                    bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
                )
                print(f"Sent new listing notification to chat_id {chat_id} for {city}.")
            except Exception as e:
                print(f"⚠️  Failed to notify {chat_id} for city {city}: {e}")


def sync_listings(scraped_items):
    """
    Compares scraped items with existing DB listings.
    Inserts new ones, updates 'last_seen' for existing ones,
    and deletes old ones based on a grace period.
    """
    current_scraped_links = {item["link"] for item in scraped_items}

    # Load existing links and their last_seen timestamp from DB
    cur.execute("SELECT link, last_seen FROM listings")
    existing_db_data = {r[0]: r[1] for r in cur.fetchall()} # {link: last_seen_timestamp}
    existing_db_links = set(existing_db_data.keys())

    # --- Process New Listings and Update Existing Ones ---
    new_items_to_add = []
    links_to_update_last_seen = []

    for item in scraped_items:
        if item["link"] not in existing_db_links:
            new_items_to_add.append(item)
        else:
            # If the listing was found in the current scrape, it means it's still active.
            # We'll update its last_seen timestamp to 'now()'.
            links_to_update_last_seen.append(item["link"])
    
    # Insert new listings
    if new_items_to_add:
        for apt in new_items_to_add:
            cur.execute("""
                INSERT INTO listings (link, title, location, city, rent, service, last_seen)
                VALUES (%s,%s,%s,%s,%s,%s,now())
            """, (
                apt["link"],
                apt["title"],
                apt["location"],
                apt["city"],
                apt["rent"],
                apt["service"]
            ))
        print(f"{datetime.now()}: Added {len(new_items_to_add)} new listings.")
        conn.commit() # Commit new insertions immediately for notification
        notify_new_listings(new_items_to_add) # Notify about newly added listings
    else:
        print(f"{datetime.now()}: No new listings found.")

    # Update last_seen for listings that were found in this scrape
    if links_to_update_last_seen:
        # Using a single UPDATE statement for efficiency
        cur.execute("""
            UPDATE listings
            SET last_seen = now()
            WHERE link = ANY(%s)
        """, (list(links_to_update_last_seen),))
        conn.commit()
        # print(f"Updated last_seen for {len(links_to_update_last_seen)} listings.") # Optional debug

    # --- Process Listings for Deletion (Grace Period Logic) ---
    # Define your grace period. This should be longer than your typical scrape interval
    # to allow for temporary scrape failures. E.g., for daily runs, 24-48 hours.
    GRACE_PERIOD_HOURS = 24 # Listings not seen for this long will be deleted.
    
    # Identify listings in DB that were NOT in the current scrape.
    # These are "potentially old" and will be checked against the grace period.
    potentially_old_links = existing_db_links - current_scraped_links

    if potentially_old_links:
        # Get the actual last_seen times for these potentially old listings from the DB
        cur.execute("SELECT link, last_seen FROM listings WHERE link = ANY(%s)", (list(potentially_old_links),))
        potentially_old_data = {r[0]: r[1] for r in cur.fetchall()}

        # Now, filter these down to truly old listings based on the grace period
        truly_deleted_links = []
        # Ensure current_time is timezone-aware for correct comparison with DB's TIMESTAMPTZ
        current_time_utc = datetime.now(timezone.utc) 

        for link, last_seen_ts in potentially_old_data.items():
            # last_seen_ts from TIMESTAMPTZ is usually timezone-aware by psycopg2
            # Compare current UTC time with the listing's last_seen timestamp
            if (current_time_utc - last_seen_ts) > timedelta(hours=GRACE_PERIOD_HOURS):
                truly_deleted_links.append(link)

        if truly_deleted_links:
            cur.execute("DELETE FROM listings WHERE link = ANY(%s)", (truly_deleted_links,))
            conn.commit()
            print(f"{datetime.now()}: Deleted {len(truly_deleted_links)} old listings from DB after grace period.")
        else:
            print(f"{datetime.now()}: No listings older than grace period to delete.")
            # Optionally, log which listings are "pending deletion" if you want to track them
            # print(f"Listings potentially old (waiting for grace period): {list(potentially_old_links)}")
    else:
        print(f"{datetime.now()}: No listings in DB that weren't found in current scrape.")


def main():
    """
    Main function to orchestrate the scraping, syncing, and notification process.
    """
    driver = None # Initialize driver to None
    try:
        driver = get_driver() # Get the Selenium WebDriver with proxy
        wait   = WebDriverWait(driver, 10) # Set up WebDriverWait
        
        listings = scrape_listings(driver, wait) # Perform the scraping
        print(f"Scraped {len(listings)} listings total.")
        
        # --- NEW: Scrape Health Check ---
        # Adjust this value based on the typical minimum number of listings expected on the site.
        # This prevents accidental mass deletions if a scrape severely fails and returns very few items.
        # For 'roofz.eu', if there are typically 50+ listings, setting this to 20 or 30 might be reasonable.
        MIN_EXPECTED_LISTINGS = 2 
        
        if len(listings) < MIN_EXPECTED_LISTINGS:
            print(f"WARNING: Scraped only {len(listings)} listings, which is unusually low "
                  f"(expected at least {MIN_EXPECTED_LISTINGS}). "
                  "This might indicate a partial or failed scrape. "
                  "Skipping the entire sync process to prevent accidental data loss. "
                  "Try again on the next scheduled run.")
            # If the scrape results are unreliable, it's safer to not touch the DB at all
            # and wait for a more successful scrape on the next run.
            return # Exit main function
            
        sync_listings(listings) # Synchronize DB with scraped data
    except Exception as e:
        print(f"Error during main execution: {e}")
        # Optionally, send an error message to an admin chat_id (uncomment and configure in .env)
        # ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
        # if ADMIN_CHAT_ID:
        #     loop.run_until_complete(
        #         bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"🔴 Scraper Error: {e}")
        #     )
    finally:
        if driver:
            driver.quit() # Always close the browser
        if cur:
            cur.close()   # Close cursor
        if conn:
            conn.close()  # Close DB connection
        print("Script finished. Browser and DB connection closed.")


if __name__ == "__main__":
    main()
