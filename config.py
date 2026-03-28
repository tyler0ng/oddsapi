"""
Configuration for the 1xBet Basketball Odds Scraper.
Update cookies/mirrors here when they rotate.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONNECTION SETTINGS
# ============================================================

BASE_URL = os.environ.get("XBET_BASE_URL", "https://1xbet.tz")

COOKIES = {
    "SESSION": os.environ.get("XBET_SESSION", ""),
    "PAY_SESSION": os.environ.get("XBET_PAY_SESSION", ""),
    "che_g": os.environ.get("XBET_CHE_G", ""),
    "auid": os.environ.get("XBET_AUID", ""),
    "lng": "en",
    "application_locale": "en",
    "coefview": "0",
    "platform_type": "mobile",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6_2 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.6 Mobile/15E148 Safari/604.1"
    ),
    "x-app-n": "__BETTING_APP__",
    "x-requested-with": "XMLHttpRequest",
    "Referer": f"{BASE_URL}/en/line/basketball",
}

# NOTE: Do NOT use a shared params dict and merge with **spread.
# This causes 406 errors with 1xBet's API for unknown reasons.
# Always use inline params in each endpoint function (see scraper.py).

# ============================================================
# SCHEDULER SETTINGS
# ============================================================

# How often to poll (seconds)
POLL_INTERVAL_DEFAULT = 300       # 5 min for games > 2 hours away
POLL_INTERVAL_APPROACHING = 120   # 2 min for games < 2 hours away
POLL_INTERVAL_IMMINENT = 60       # 1 min for games < 30 min away

# Delay between individual API calls (be polite)
REQUEST_DELAY = 1.0

# Max retries per request before skipping
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retries

# ============================================================
# DATABASE
# ============================================================

DB_PATH = os.environ.get("DB_PATH", "odds_tracker.db")

# ============================================================
# API-BASKETBALL (for game results)
# ============================================================

API_BASKETBALL_KEY = os.environ.get("API_BASKETBALL_KEY", "")
API_BASKETBALL_BASE = "https://v1.basketball.api-sports.io"

# How often to check for results (seconds)
RESULTS_CHECK_INTERVAL = 3600  # 1 hour

# ============================================================
# TARGET LEAGUES — only these get polled for odds
# ============================================================

TARGET_LEAGUES = {
    # Major
    "NBA": 13589,
    "Euroleague": 139460,
    "NCAA": 124789,

    # Asian markets
    "South Korea KBL": 118927,
    "China NBL": 62469,
    "Indonesia IBL": 34445,
    "Taiwan P League+": 2153164,
    "Hong Kong Championship": 986729,

    # Other small markets
    "Australia NBL": 220263,
    "Brazil NBB": 24669,
    "Argentina LNB": 24591,
    "Uruguay Championship": 34691,
    "Venezuela Championship": 28903,
    "Mexico CIBACOPA": 2579642,

    # European small markets
    "Finland Korisliiga": 24745,
    "Sweden Basketligan": 24871,
    "Slovenia Liga": 27045,
    "Denmark Basketligaen": 24707,
    "Georgia Superliga": 133487,
    "Montenegro Erste Liga": 177349,
}

# ============================================================
# MARKET / ODDS REFERENCE MAPS
# ============================================================

MARKET_TYPES = {
    1: "Home Win (1X2)",
    2: "Draw (1X2)",
    3: "Away Win (1X2)",
    4: "Home Win (12)",
    6: "Away Win (12)",
    7: "Home Handicap",
    8: "Away Handicap",
    9: "Total Over",
    10: "Total Under",
    11: "Home Total Over",
    12: "Home Total Under",
    13: "Away Total Over",
    14: "Away Total Under",
    182: "1st Half Odd",
    183: "1st Half Even",
    1052: "Odd",
    1053: "Even",
}

GROUP_NAMES = {
    1: "1X2",
    8: "Moneyline",
    2: "Handicap",
    17: "Total",
    15: "Home Total",
    62: "Away Total",
    230: "Odd/Even",
    14: "1st Half Odd/Even",
    27: "Quarter Handicap",
    60: "Winning Margin",
}

# Which market group IDs to store (None = store everything)
TARGET_MARKET_GROUPS = None  # or e.g. {8, 2, 17, 15, 62} for moneyline/handicap/totals only

# ============================================================
# LEAGUE EXCLUSIONS — leagues to skip during scraping
# ============================================================
# Add league names exactly as they appear on 1xBet (run `python query.py leagues`
# or fetch_all_leagues() to find exact names).
# Useful for: sharp/efficient leagues with little EV, high-volume leagues
# that bloat your DB without adding model value.

EXCLUDED_LEAGUES = {
    "NBA",
    # Add more as needed — use exact names from `python query.py leagues`
}

# ============================================================
# TELEGRAM NOTIFICATIONS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
