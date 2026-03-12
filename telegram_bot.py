#!/usr/bin/env python3
import os
import psycopg2
from dotenv import load_dotenv

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)
# No need for datetime or timedelta here if not doing time comparisons,
# but keeping it consistent with the scraper.
from datetime import datetime, timedelta, timezone # Added timezone for robustness


# — Load config —
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DB_URL         = os.getenv("DATABASE_URL")
if not TELEGRAM_TOKEN or not DB_URL:
    raise RuntimeError("Zorg dat TELEGRAM_TOKEN en DATABASE_URL in .env staan")

def get_db_conn():
    return psycopg2.connect(DB_URL)

def get_cities():
    with get_db_conn() as conn, conn.cursor() as cur:
        # Use last_seen from the new schema
        cur.execute("SELECT DISTINCT city FROM listings ORDER BY city")
        return [row[0] for row in cur.fetchall()]

def add_subscription(chat_id: int, city: str):
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO subscriptions (chat_id, city) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (chat_id, city)
        )
        conn.commit() # Commit changes to DB

def remove_subscription(chat_id: int, city: str):
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM subscriptions WHERE chat_id=%s AND city=%s",
            (chat_id, city)
        )
        conn.commit() # Commit changes to DB

def list_subscriptions(chat_id: int):
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT city FROM subscriptions WHERE chat_id=%s", (chat_id,))
        return [r[0] for r in cur.fetchall()]

def get_listings_for(city: str, limit: int = 20): # Added limit parameter
    """Fetch all listings for a city, limited to avoid too-long messages."""
    with get_db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT title, rent, service, link
              FROM listings
             WHERE city=%s
          ORDER BY last_seen DESC -- CHANGED: From scraped_at to last_seen
             LIMIT %s             -- ADDED: Limit the number of results
        """, (city, limit))
        return cur.fetchall()

def build_main_menu():
    kb = [
        [InlineKeyboardButton("➕ Abonneren",        callback_data="menu_sub")],
        [InlineKeyboardButton("➖ Afmelden",          callback_data="menu_unsub")],
        [InlineKeyboardButton("📋 Mijn abonnementen", callback_data="menu_list")],
        [InlineKeyboardButton("🏠 Aanbod",            callback_data="menu_aanbod")],
    ]
    return InlineKeyboardMarkup(kb)

WELCOME_TEXT = (
    "👋 Welkom bij de Roofz-bot!\n\n"
    "Kies een optie:\n"
    "➕ Abonneren – meld je aan voor nieuwe listings in een stad\n"
    "➖ Afmelden – stop je abonnement in een stad\n"
    "📋 Mijn abonnementen – bekijk je abonnementen\n"
    "🏠 Aanbod – zie alle huidige listings per stad"
)

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        WELCOME_TEXT,
        reply_markup=build_main_menu()
    )

async def button_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    data = q.data
    chat = q.message.chat_id
    await q.answer()

    # — Main menu redisplay —
    if data in ("menu_back", "menu_back_to_main"):
        # Edit the message if it's the same message to avoid new message spam
        if q.message.reply_markup != build_main_menu(): # Check if current message is not already main menu
            await q.edit_message_text(
                WELCOME_TEXT,
                reply_markup=build_main_menu()
            )
        else: # If already main menu, just send new message or do nothing
            await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT, reply_markup=build_main_menu())
        return

    # — List subscriptions (final) —
    if data == "menu_list":
        subs = list_subscriptions(chat)
        text = "Je hebt geen abonnementen." if not subs else \
               "🔔 Je abonnementen:\n" + "\n".join(f"• {s}" for s in subs)
        await q.edit_message_text(text) # Edit the query message instead of sending new
        # then show main menu after a short delay or as a separate message
        await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT,
            reply_markup=build_main_menu())
        return

    # — Offerings submenu —
    if data == "menu_aanbod":
        cities = get_cities()
        if not cities:
            await q.edit_message_text("Geen steden gevonden in het aanbod. De database is mogelijk leeg.")
            await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT, reply_markup=build_main_menu())
            return
        kb = [[InlineKeyboardButton(c, callback_data=f"show:{c}")] for c in cities]
        kb.append([InlineKeyboardButton("🏡 Hoofdmenu", callback_data="menu_back")])
        await q.edit_message_text(
            "🏠 Aanbod: kies een stad om de meest recente listings te zien:", # Updated text
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    # — Subscribe submenu —
    if data == "menu_sub":
        cities = get_cities()
        if not cities:
            await q.edit_message_text("Geen steden gevonden om op te abonneren. De database is mogelijk leeg.")
            await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT, reply_markup=build_main_menu())
            return
        kb = [[InlineKeyboardButton(c, callback_data=f"sub:{c}")] for c in cities]
        kb.append([InlineKeyboardButton("🏡 Hoofdmenu", callback_data="menu_back")])
        await q.edit_message_text(
            "➕ Abonneren: selecteer een stad:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    # — Unsubscribe submenu —
    if data == "menu_unsub":
        subs = list_subscriptions(chat)
        if not subs:
            await q.edit_message_text("Je hebt geen abonnementen om op te zeggen.") # Edit message
            await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT,
                reply_markup=build_main_menu())
            return
        kb = [[InlineKeyboardButton(c, callback_data=f"unsub:{c}")] for c in subs]
        kb.append([InlineKeyboardButton("🏡 Hoofdmenu", callback_data="menu_back")])
        await q.edit_message_text(
            "➖ Afmelden: selecteer een stad om te stoppen:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    # — Subscribe action (final) —
    if data.startswith("sub:"):
        city = data.split(":",1)[1]
        add_subscription(chat, city)
        await q.edit_message_text( # Edit message
            text=f"✅ Je bent nu geabonneerd op *{city}*",
            parse_mode="Markdown")
        await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT,
            reply_markup=build_main_menu())
        return

    # — Unsubscribe action (final) —
    if data.startswith("unsub:"):
        city = data.split(":",1)[1]
        remove_subscription(chat, city)
        await q.edit_message_text( # Edit message
            text=f"❌ Je abonnement op *{city}* is verwijderd",
            parse_mode="Markdown")
        await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT,
            reply_markup=build_main_menu())
        return

    # — Show all listings (final) —
    if data.startswith("show:"):
        city = data.split(":",1)[1]
        rows = get_listings_for(city) # Uses the updated function with LIMIT
        if not rows:
            text = f"Geen recente listings gevonden voor *{city}*."
        else:
            text = f"🏡 *De {len(rows)} meest recente listings in {city}:*\n\n" + "\n\n".join(
                f"[{t}]({l}) — {r} + {s}"
                for t, r, s, l in rows
            )
        await q.edit_message_text(text, parse_mode="Markdown") # Edit message
        await ctx.bot.send_message(chat_id=chat, text=WELCOME_TEXT,
            reply_markup=build_main_menu())
        return

async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Gebruik /start om het menu te openen.")

def main():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    print("🚀 Telegram bot gestart met buttons …")
    app.run_polling()

if __name__ == "__main__":
    main()
