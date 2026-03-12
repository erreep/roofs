#!/usr/bin/env python3
import json
import time, random

# This initial sleep helps vary start times if you run multiple instances on a schedule.
time.sleep(random.uniform(10, 30))

import os
import psycopg2
import asyncio
from datetime import datetime, timedelta, timezone # Added timedelta and timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin
from urllib.request import ProxyHandler, Request, build_opener

from dotenv import load_dotenv
load_dotenv()  # loads DATABASE_URL, TELEGRAM_TOKEN, and PROXY variables from .env

from telegram import Bot

# --- CONFIG & DB SETUP ---
DB_URL         = os.getenv("DATABASE_URL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BASE_URL       = "https://roofz.eu"
PROPERTY_OVERVIEW_PATH = "/huur/woningen"
PROPERTY_API_PATH = "/api/ms/listing/properties"
STORYBLOK_PAGES_PATH = "/api/ms/storyblok/pages"
PROPERTY_OVERVIEW_CONTENT_TYPE = "sb-property-overview-page"

# PROXY CONFIGURATION - Read from .env
# PROXY_LIST_STR: Comma-separated list of proxies in "host:port:user:pass" format
PROXY_LIST_STR = os.getenv("PROXY_LIST")

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


def get_http_opener():
    """
    Builds an opener for Roofz API calls, optionally routed through a random proxy.
    """
    handlers = []

    if PROXY_LIST_STR:
        raw_proxy_strings = [p.strip() for p in PROXY_LIST_STR.split(",") if p.strip()]
        if raw_proxy_strings:
            selected_raw_proxy = random.choice(raw_proxy_strings)
            parts = selected_raw_proxy.split(":")
            if len(parts) == 4:
                host, port, user, password = parts
                proxy_url = (
                    f"http://{quote(user, safe='')}:{quote(password, safe='')}"
                    f"@{host}:{port}"
                )
                handlers.append(ProxyHandler({
                    "http": proxy_url,
                    "https": proxy_url,
                }))
                print(f"Using proxy for API requests: {host}:{port} with user '{user}' (selected from list).")
            else:
                print(
                    f"WARNING: Invalid proxy string format in PROXY_LIST: {selected_raw_proxy}. "
                    "Expected 'host:port:user:pass'. Running without proxy."
                )
        else:
            print("WARNING: PROXY_LIST is set but contains no valid proxy entries. Running without proxy.")
    else:
        print("INFO: PROXY_LIST is not set. Running without proxy.")

    return build_opener(*handlers)


def fetch_json(opener, path, params=None, timeout=30):
    """
    Fetches JSON from a Roofz API endpoint.
    """
    url = urljoin(BASE_URL, path)
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"

    req = Request(url, headers={
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    })

    try:
        with opener.open(req, timeout=timeout) as response:
            status = getattr(response, "status", response.getcode())
            body = response.read()
    except HTTPError as exc:
        if exc.code == 204:
            return None
        print(f"HTTP error while fetching {url}: {exc}")
        return None
    except URLError as exc:
        print(f"Network error while fetching {url}: {exc}")
        return None

    if status == 204 or not body:
        return None

    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        print(f"Failed to parse JSON from {url}: {exc}")
        return None


def iter_storyblok_components(node):
    """
    Yields nested Storyblok components from a page payload.
    """
    if isinstance(node, dict):
        if "component" in node:
            yield node
        for value in node.values():
            yield from iter_storyblok_components(value)
    elif isinstance(node, list):
        for item in node:
            yield from iter_storyblok_components(item)


def load_scrape_config(opener):
    """
    Loads the overview page config so the scraper follows the site's live API filters.
    """
    payload = fetch_json(opener, STORYBLOK_PAGES_PATH, {
        "perPage": 1,
        "version": "published",
        "content_type": PROPERTY_OVERVIEW_CONTENT_TYPE,
    })

    property_type = "RentResident"
    default_filters = {"stage": "available"}

    stories = (payload or {}).get("stories") or []
    if not stories:
        print("WARNING: Could not load Storyblok overview config. Falling back to default API filters.")
        return property_type, default_filters

    page_content = stories[0].get("content") or {}
    property_type = page_content.get("propertyType") or property_type

    for component in iter_storyblok_components(page_content):
        if component.get("component") != "sb-properties-overview":
            continue

        raw_filters = component.get("defaultFilters")
        if raw_filters:
            try:
                parsed_filters = json.loads(raw_filters)
                if isinstance(parsed_filters, dict):
                    default_filters.update(parsed_filters)
            except json.JSONDecodeError:
                print(f"WARNING: Could not parse defaultFilters JSON: {raw_filters!r}")
        break

    if isinstance(default_filters.get("stage"), list) and len(default_filters["stage"]) == 1:
        default_filters["stage"] = default_filters["stage"][0]

    return property_type, default_filters


def build_property_query(property_type, default_filters, page, per_page):
    """
    Converts the site filter config into query params accepted by the public JSON API.
    """
    params = {
        "page": page,
        "perPage": per_page,
        "filter[import_type]": property_type,
    }

    for key, value in (default_filters or {}).items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            value = ",".join(str(item) for item in value)
        params[f"filter[{key}]"] = value

    return params


def build_location(address):
    """
    Formats a human-readable location string from the API address payload.
    """
    address = address or {}

    street_line = " ".join(
        part for part in [
            address.get("street", "").strip(),
            address.get("house_number", "").strip(),
            address.get("house_number_extension", "").strip(),
        ]
        if part
    )
    city_line = " ".join(
        part for part in [
            address.get("postal_code", "").strip(),
            address.get("location", "").strip(),
        ]
        if part
    )

    return ", ".join(part for part in [street_line, city_line] if part)


def build_listing_link(item):
    """
    Builds the canonical listing URL used on the live Roofz site.
    """
    if item.get("external_url"):
        return item["external_url"]

    slug = item.get("slug")
    if not slug:
        return None

    return urljoin(BASE_URL, f"{PROPERTY_OVERVIEW_PATH.rstrip('/')}/{slug}")


def scrape_listings(opener):
    """
    Fetches listings from the live Roofz JSON API instead of scraping rendered HTML.
    """
    property_type, default_filters = load_scrape_config(opener)
    print(
        f"Fetching listings from API using property type '{property_type}' "
        f"and filters {default_filters}."
    )

    all_items = []
    page = 1
    last_page = 1
    per_page = 100

    while page <= last_page:
        payload = fetch_json(
            opener,
            PROPERTY_API_PATH,
            build_property_query(property_type, default_filters, page, per_page),
        )

        if not payload:
            if page == 1:
                print("No listing payload returned from API.")
            break

        page_items = payload.get("data") or []
        meta = payload.get("meta") or {}
        last_page = int(meta.get("last_page") or 1)

        for item in page_items:
            link = build_listing_link(item)
            if not link:
                print(f"WARNING: Skipping listing without slug/link: {item.get('id')}")
                continue

            address = item.get("address") or {}
            handover = item.get("handover") or {}

            all_items.append({
                "title": item.get("title", "").strip(),
                "location": build_location(address),
                "city": (address.get("location") or "").strip(),
                "rent": (handover.get("price_formatted") or "").strip(),
                "service": (handover.get("service_costs_formatted") or "").strip(),
                "link": link,
            })

        print(f"Fetched {len(page_items)} listings from API page {page}/{last_page}.")
        page += 1

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
    try:
        opener = get_http_opener()
        listings = scrape_listings(opener)
        print(f"Scraped {len(listings)} listings total.")
        
        # --- Scrape Health Check ---
        # Keep a low but non-zero floor so a bad API response does not wipe the database.
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
        if cur:
            cur.close()   # Close cursor
        if conn:
            conn.close()  # Close DB connection
        print("Script finished. DB connection closed.")


if __name__ == "__main__":
    main()
