"""
1xBet API scraper functions.
Cleaned-up version of the original script — returns structured data
instead of printing, so the scheduler can pipe it into the database.

IMPORTANT: Each endpoint uses inline params (not **COMMON_PARAMS merge)
because dict merging causes 406 errors with 1xBet's API for unknown reasons.
"""

import time
from curl_cffi import requests
from config import (
    BASE_URL, HEADERS,
    MARKET_TYPES, GROUP_NAMES, TARGET_MARKET_GROUPS,
    MAX_RETRIES, RETRY_DELAY, REQUEST_DELAY,
)


# ============================================================
# LOW-LEVEL REQUEST
# ============================================================

def api_request(url, params=None, retries=MAX_RETRIES):
    """
    Make a request to 1xBet with retry logic and TLS impersonation.
    Returns parsed JSON or None on failure.
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                url,
                params=params,
                headers=HEADERS,
                impersonate="safari17_0",
                timeout=15,
            )

            if response.status_code == 200:
                return response.json()

            # Rate limited or server error — retry
            if response.status_code in (429, 500, 502, 503):
                print(f"  [RETRY {attempt}/{retries}] Status {response.status_code}, waiting {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY * attempt)
                continue

            # Client error (403, 401) — likely cookie/session issue
            if response.status_code in (401, 403):
                print(f"  [AUTH ERROR] Status {response.status_code} — cookies may have expired")
                return None

            print(f"  [ERROR] Status {response.status_code} for {url}")
            return None

        except Exception as e:
            print(f"  [REQUEST ERROR attempt {attempt}/{retries}] {e}")
            if attempt < retries:
                time.sleep(RETRY_DELAY)

    print(f"  [FAILED] All {retries} attempts exhausted for {url}")
    return None


# ============================================================
# API ENDPOINTS
# ============================================================

def fetch_all_leagues():
    """
    Fetch all available basketball leagues.
    Returns: list of {"name": str, "id": int, "game_count": int}
    """
    url = f"{BASE_URL}/service-api/LineFeed/GetSportsShortZip"
    params = {
        "sports": "3",
        "lng": "en",
        "country": "180",
        "partner": "398",
        "virtualSports": "true",
        "gr": "1501",
        "groupChamps": "true",
    }

    data = api_request(url, params)
    if not data:
        return []

    leagues = []
    for sport in data.get("Value", []):
        if sport.get("I") != 3:
            continue
        for league in sport.get("L", []):
            if "SC" in league:
                for sub in league["SC"]:
                    leagues.append({
                        "name": sub["L"],
                        "id": sub["LI"],
                        "game_count": sub.get("GC", 0),
                    })
            else:
                leagues.append({
                    "name": league["L"],
                    "id": league["LI"],
                    "game_count": league.get("GC", 0),
                })

    return leagues


def fetch_league_games(league_id, league_name=""):
    """
    Fetch all upcoming games for a league.
    Returns: list of game dicts with game_id, teams, start_time, etc.
    """
    url = f"{BASE_URL}/service-api/LineFeed/GetChampZip"
    params = {
        "champ": str(league_id),
        "lng": "en",
        "country": "180",
        "partner": "398",
        "gr": "1501",
    }

    data = api_request(url, params)
    if not data or not data.get("Success"):
        return []

    value = data.get("Value")
    if not value:
        return []

    games = []
    for g in value.get("G", []):
        home = g.get("O1", "Unknown")
        away = g.get("O2", "Unknown")
        if home == "Unknown" and away == "Unknown":
            continue

        games.append({
            "game_id": g.get("I"),
            "home_team": home,
            "away_team": away,
            "start_time": g.get("S"),
            "league_name": league_name,
            "league_id": league_id,
        })

    return games


def fetch_game_odds(game_id):
    """
    Fetch all markets/odds for a specific game.
    Returns: raw API response dict, or None.
    """
    url = f"{BASE_URL}/service-api/main-line-feed/v1/gameEvents"
    params = {
        "cfView": "3",
        "countEvents": "250",
        "country": "180",
        "gameId": str(game_id),
        "gr": "1501",
        "grMode": "4",
        "lng": "en",
        "marketType": "1",
        "ref": "398",
    }

    return api_request(url, params)


# ============================================================
# PARSING
# ============================================================

def parse_odds(raw_data):
    """
    Parse raw API odds response into a clean dict.
    """
    if not raw_data:
        return None

    result = {
        "game_id": raw_data.get("id"),
        "game_name": raw_data.get("fullName", ""),
        "period": raw_data.get("periodName", "Full Game"),
        "start_ts": raw_data.get("startTs"),
        "total_markets": raw_data.get("eventsCount", 0),
        "markets": {},
    }

    for group in raw_data.get("eventGroups", []):
        group_id = group.get("groupId")

        if TARGET_MARKET_GROUPS and group_id not in TARGET_MARKET_GROUPS:
            continue

        group_name = GROUP_NAMES.get(group_id, f"Group_{group_id}")
        entries = []

        for side in group.get("events", []):
            for outcome in side:
                entry = {
                    "type": outcome.get("type"),
                    "type_name": MARKET_TYPES.get(
                        outcome.get("type"),
                        f"Type_{outcome.get('type')}"
                    ),
                    "odds": outcome.get("cf"),
                    "odds_display": outcome.get("cfView"),
                    "line": outcome.get("parameter"),
                    "is_main_line": outcome.get("isCenter", False),
                }
                entries.append(entry)

        if entries:
            result["markets"][group_name] = entries

    return result


# ============================================================
# CONVENIENCE
# ============================================================

def fetch_and_parse_game_odds(game_id):
    """Fetch + parse in one call."""
    raw = fetch_game_odds(game_id)
    if raw:
        return parse_odds(raw)
    return None
