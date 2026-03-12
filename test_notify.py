#!/usr/bin/env python3
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# load TELEGRAM_TOKEN + DATABASE_URL
load_dotenv()

from roofz_bot import notify_new_listings  # import the function you already have

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL missing in .env")

# Connect and ensure you have a subscription (optional check)
conn = psycopg2.connect(DB_URL)
cur  = conn.cursor()
cur.execute("SELECT chat_id FROM subscriptions WHERE city='Amsterdam'")
rows = cur.fetchall()
if not rows:
    print("⚠️  You have no subscriptions for Amsterdam – send /subscribe first!")
    exit(1)

# Construct a fake listing
fake = [{
    "link":     "https://www.roofz.eu/availability/fake-test-123",
    "title":    "TEST Appartement 123",
    "location": "0000 ZZ, Amsterdam",
    "city":     "Amsterdam",
    "rent":     "€ 0,00",
    "service":  "€ 0,00"
}]

print(f"{datetime.now()}: Sending test notification…")
notify_new_listings(fake)
print("Done.")
