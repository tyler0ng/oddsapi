"""
1xBet Basketball Odds Scraper
==============================
Built from real API endpoints discovered via mitmproxy.

ENDPOINTS:
  1. GetSportsShortZip  → List all basketball leagues + their IDs
  2. GetChampZip        → List all games in a specific league
  3. gameEvents         → Get full odds for a specific game

USAGE:
  1. Replace BASE_URL with your working 1xBet mirror domain
  2. Replace COOKIES with fresh cookies from mitmproxy
  3. Run the script!

NOTES:
  - Sessions expire. When you start getting errors, recapture
    cookies from mitmproxy.
  - The mirror domain (BASE_URL) may change. Update as needed.
  - 1xBet uses "cf" for coefficient (decimal odds).
"""
# Replace this:
import json
from curl_cffi import requests
import time
from datetime import datetime

# ============================================================
# CONFIGURATION — UPDATE THESE VALUES
# ============================================================

# Your working 1xBet mirror domain (change when it rotates)
BASE_URL = "https://1xbet.tz"

# Cookies from mitmproxy (update when session expires)
COOKIES = {
    "SESSION": "3a71ecea7f82b99e161787674eb53843",
    "PAY_SESSION": "ad3a87278f30549fcecc90fa3b2ebd0c",
    "che_g": "35b62414-bab2-424e-b711-615ea2d48aa3",
    "auid": "W7rP4mmu0u8cv0qxBHr/Ag==",
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
    "Referer": "https://1xbet.tz/en/line/basketball",
}
# Common query parameters used across endpoints
COMMON_PARAMS = {
    "lng": "en",
    "country": "180",       # Can be any country code
    "partner": "398",       # Referral code from your session
    "gr": "1501",
}


# ============================================================
# LEAGUE IDs — Discovered from 1xBet API
# ============================================================
# Add/remove leagues based on what you want to track

BASKETBALL_LEAGUES = {
    # Major leagues
    "NBA": 13589,
    "Euroleague": 139460,
    "NCAA": 124789,

    # Asian leagues (your target markets)
    "South Korea KBL": 118927,
    "China NBL": 62469,
    "Indonesia IBL": 34445,
    "Taiwan Championship": 1694120,
    "Hong Kong Championship": 986729,

    # Philippines — IDs from browser URLs / live section
    # These may only appear when games are scheduled
    "Philippines MPBL": 2513336,
    # Add PBA league ID here when you discover it in mitmproxy

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
    "Bulgaria NBL": 197105,
}


# ============================================================
# MARKET TYPE MAPPING — What the "type" codes mean
# ============================================================

MARKET_TYPES = {
    # Main game markets
    1: "Home Win (1X2)",
    2: "Draw (1X2)",
    3: "Away Win (1X2)",
    4: "Home Win (12)",
    6: "Away Win (12)",

    # Totals
    9: "Total Over",
    10: "Total Under",

    # Handicap
    7: "Home Handicap",
    8: "Away Handicap",

    # Home team total
    11: "Home Total Over",
    12: "Home Total Under",

    # Away team total
    13: "Away Total Over",
    14: "Away Total Under",

    # Odd/Even
    182: "1st Half Odd",
    183: "1st Half Even",
    1052: "Odd",
    1053: "Even",
}

GROUP_NAMES = {
    1: "1X2 (Moneyline with Draw)",
    8: "12 (Moneyline)",
    2: "Handicap",
    17: "Total",
    15: "Home Team Total",
    62: "Away Team Total",
    230: "Odd/Even",
    14: "1st Half Odd/Even",
    27: "Quarter Score Handicap",
    60: "Winning Margin",
    88: "Home Team Individual Total by Halves",
    89: "Away Team Individual Total by Halves",
    91: "1st Half Odd/Even Home",
    92: "1st Half Odd/Even Away",
    99: "Asian Total",
}


# ============================================================
# API FUNCTIONS
# ============================================================



def make_request(url, params=None):
    """Make a request to 1xBet API using curl_cffi to bypass TLS fingerprinting."""
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
        else:
            print(f"  ERROR: Status {response.status_code}")
            print(f"  URL: {response.url}")
            return None

    except Exception as e:
        print(f"  REQUEST ERROR: {e}")
        return None

def get_all_basketball_leagues():
    print("=" * 60)
    print("Fetching all basketball leagues...")
    print("=" * 60)

    response = requests.get(
        f"{BASE_URL}/service-api/LineFeed/GetSportsShortZip",
        params={
            "sports": "3",
            "lng": "en",
            "country": "180",
            "partner": "398",
            "virtualSports": "true",
            "gr": "1501",
            "groupChamps": "true",
        },
        headers=HEADERS,
        impersonate="safari17_0",
    )

    if response.status_code != 200:
        print(f"  ERROR: Status {response.status_code}")
        return []

    data = response.json()

    leagues = []
    for sport in data.get("Value", []):
        if sport.get("I") == 3 and sport.get("N") == "Basketball":
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

    print(f"\nFound {len(leagues)} basketball leagues:\n")
    for lg in leagues:
        print(f"  {lg['name']} | ID: {lg['id']} | Games: {lg['game_count']}")

    return leagues

def get_league_games(league_id, league_name=""):
    """
    Fetch all upcoming games for a specific league.
    Endpoint: GetChampZip
    """
    print(f"\n{'=' * 60}")
    print(f"Fetching games for: {league_name} (ID: {league_id})")
    print("=" * 60)

    url = f"{BASE_URL}/service-api/LineFeed/GetChampZip"
    params = {
        **COMMON_PARAMS,
        "champ": str(league_id),
        "virtualSports": "true",
    }

    data = make_request(url, params)

    if not data or not data.get("Success"):
        print(f"  No games found or request failed")
        return []

    games = []
    for game_data in data.get("Value", []):
        # Extract game info — field names are abbreviated
        game = {
            "game_id": game_data.get("CI"),       # Game ID
            "home_team": game_data.get("O1"),      # Team 1 (home)
            "away_team": game_data.get("O2"),      # Team 2 (away)
            "start_time": game_data.get("S"),      # Start timestamp
            "league": league_name,
            "league_id": league_id,
        }

        # Convert timestamp to readable date
        if game["start_time"]:
            try:
                game["start_datetime"] = datetime.fromtimestamp(
                    game["start_time"]
                ).strftime("%Y-%m-%d %H:%M")
            except (ValueError, OSError):
                game["start_datetime"] = "Unknown"

        # Only include if we have team names
        if game["home_team"] and game["away_team"]:
            games.append(game)

    print(f"\n  Found {len(games)} upcoming games:\n")
    for g in games:
        print(f"  {g.get('start_datetime', '?')} | {g['home_team']} vs {g['away_team']}")
        print(f"    Game ID: {g['game_id']}")

    return games


def get_game_odds(game_id, market_type=1):
    """
    Fetch full odds for a specific game.
    Endpoint: gameEvents

    market_type options:
      1 = All markets (121+ betting options)
      2 = Popular
      3 = Total
      4 = Handicap
      13 = Asian markets
    """
    url = f"{BASE_URL}/service-api/main-line-feed/v1/gameEvents"
    params = {
        **COMMON_PARAMS,
        "cfView": "3",
        "countEvents": "250",
        "gameId": str(game_id),
        "grMode": "4",
        "marketType": str(market_type),
        "ref": "398",
    }

    data = make_request(url, params)

    if not data:
        print(f"  No odds found for game {game_id}")
        return None

    return data


def parse_odds(raw_data):
    """
    Parse the raw odds JSON into a clean, readable format.
    Returns a dictionary of markets and their odds.
    """
    if not raw_data:
        return {}

    result = {
        "game_id": raw_data.get("id"),
        "game_name": raw_data.get("fullName", ""),
        "period": raw_data.get("periodName", "Full Game"),
        "start_timestamp": raw_data.get("startTs"),
        "total_markets": raw_data.get("eventsCount", 0),
        "markets": {},
    }

    # Parse sub-games (quarters, halves)
    result["sub_games"] = []
    for sg in raw_data.get("subGamesForMainGame", []):
        result["sub_games"].append({
            "id": sg["id"],
            "name": sg["subGameName"],
            "period": sg["period"],
        })

    # Parse each event group (market category)
    for group in raw_data.get("eventGroups", []):
        group_id = group.get("groupId")
        group_name = GROUP_NAMES.get(group_id, f"Market Group {group_id}")

        market_entries = []

        for side in group.get("events", []):
            for outcome in side:
                entry = {
                    "type": outcome.get("type"),
                    "type_name": MARKET_TYPES.get(
                        outcome.get("type"),
                        f"Type {outcome.get('type')}"
                    ),
                    "odds": outcome.get("cf"),
                    "odds_display": outcome.get("cfView"),
                    "line": outcome.get("parameter"),
                    "is_main_line": outcome.get("isCenter", False),
                }

                # Extract params if available
                params = outcome.get("eventParams", {}).get("params", [])
                if params:
                    entry["params"] = params

                market_entries.append(entry)

        result["markets"][group_name] = market_entries

    return result


def print_odds_summary(parsed_odds):
    """Print a clean summary of parsed odds."""
    if not parsed_odds:
        print("  No odds to display")
        return

    print(f"\n  Game: {parsed_odds['game_name']}")
    print(f"  Period: {parsed_odds['period']}")
    print(f"  Total markets: {parsed_odds['total_markets']}")
    print(f"  {'-' * 50}")

    for market_name, entries in parsed_odds["markets"].items():
        print(f"\n  [{market_name}]")
        for e in entries:
            line_str = f" ({e['line']:+.1f})" if e.get("line") is not None else ""
            main = " ★" if e.get("is_main_line") else ""
            print(f"    {e['type_name']}{line_str}: {e['odds_display']}{main}")


# ============================================================
# MAIN WORKFLOWS
# ============================================================

def scan_all_leagues():
    """Discover all available basketball leagues."""
    leagues = get_all_basketball_leagues()
    return leagues


def scan_league_games(league_id, league_name=""):
    """Get all games for a specific league."""
    games = get_league_games(league_id, league_name)
    return games


def get_full_odds_for_game(game_id):
    """Get and display all odds for a specific game."""
    print(f"\nFetching odds for game {game_id}...")

    raw = get_game_odds(game_id, market_type=1)  # 1 = all markets
    if raw:
        parsed = parse_odds(raw)
        print_odds_summary(parsed)
        return parsed
    return None


def scan_league_with_odds(league_id, league_name=""):
    """
    Full pipeline: get all games for a league,
    then fetch odds for each game.
    """
    games = get_league_games(league_id, league_name)

    all_odds = []
    for game in games:
        gid = game["game_id"]
        print(f"\n{'─' * 60}")
        print(f"  {game['home_team']} vs {game['away_team']}")
        print(f"  {game.get('start_datetime', '?')}")

        raw = get_game_odds(gid)
        if raw:
            parsed = parse_odds(raw)
            parsed["home_team"] = game["home_team"]
            parsed["away_team"] = game["away_team"]
            parsed["start_datetime"] = game.get("start_datetime")
            all_odds.append(parsed)
            print_odds_summary(parsed)

        # Be polite — don't hammer the API
        time.sleep(1)

    return all_odds


def export_odds_to_json(odds_list, filename="odds_export.json"):
    """Save odds data to a JSON file for your EV model."""
    with open(filename, "w") as f:
        json.dump(odds_list, f, indent=2, default=str)
    print(f"\nExported {len(odds_list)} games to {filename}")

def get_league_games(league_id, league_name=""):
    print(f"\n{'=' * 60}")
    print(f"Fetching games for: {league_name} (ID: {league_id})")
    print("=" * 60)

    response = requests.get(
        f"{BASE_URL}/service-api/LineFeed/GetChampZip",
        params={
            "champ": str(league_id),
            "lng": "en",
            "country": "180",
            "partner": "398",
            "gr": "1501",
        },
        headers=HEADERS,
        impersonate="safari17_0",
    )

    if response.status_code != 200:
        print(f"  ERROR: Status {response.status_code}")
        return []

    data = response.json()

    if not data.get("Success"):
        print("  API returned Success=false")
        return []

    games = []
    for game_data in data.get("Value", {}).get("G", []):
        game = {
            "game_id": game_data.get("I"),
            "home_team": game_data.get("O1", "Unknown"),
            "away_team": game_data.get("O2", "Unknown"),
            "league": game_data.get("LE", league_name),
            "venue": game_data.get("MIO", {}).get("Loc", ""),
            "game_number": game_data.get("N"),
        }
        games.append(game)

    print(f"\n  Found {len(games)} games:\n")
    for g in games:
        venue = f" @ {g['venue']}" if g['venue'] else ""
        print(f"  {g['home_team']} vs {g['away_team']}{venue}")
        print(f"    Game ID: {g['game_id']}")

    return games

def get_game_odds(game_id, game_name=""):
    print(f"\n{'=' * 60}")
    print(f"Fetching odds for: {game_name} (ID: {game_id})")
    print("=" * 60)

    response = requests.get(
        f"{BASE_URL}/service-api/main-line-feed/v1/gameEvents",
        params={
            "cfView": "3",
            "countEvents": "250",
            "country": "180",
            "gameId": str(game_id),
            "gr": "1501",
            "grMode": "4",
            "lng": "en",
            "marketType": "1",
            "ref": "398",
        },
        headers=HEADERS,
        impersonate="safari17_0",
    )

    if response.status_code != 200:
        print(f"  ERROR: Status {response.status_code}")
        return None

    data = response.json()

    print(f"\n  Game: {data.get('fullName', 'N/A')}")
    print(f"  Period: {data.get('periodName', 'Full Game')}")
    print(f"  Total markets: {data.get('eventsCount', 0)}")

    # Parse key markets
    for group in data.get("eventGroups", []):
        gid = group.get("groupId")

        # Only show the main markets
        if gid == 1:
            print(f"\n  [1X2 Moneyline]")
        elif gid == 8:
            print(f"\n  [Win/Lose]")
        elif gid == 17:
            print(f"\n  [Total]")
        elif gid == 2:
            print(f"\n  [Handicap]")
        else:
            continue

        for side in group.get("events", []):
            for outcome in side:
                odds = outcome.get("cfView", "?")
                line = outcome.get("parameter")
                line_str = f" ({line:+.1f})" if line is not None else ""
                main = " ★" if outcome.get("isCenter") else ""
                type_code = outcome.get("type")

                # Label the outcome
                if type_code in [1, 4, 7]:
                    label = "Home"
                elif type_code in [3, 6, 8]:
                    label = "Away"
                elif type_code == 2:
                    label = "Draw"
                elif type_code == 9:
                    label = "Over"
                elif type_code == 10:
                    label = "Under"
                else:
                    label = f"Type {type_code}"

                print(f"    {label}{line_str}: {odds}{main}")

    return data

# ============================================================
# RUN IT
# ============================================================

if __name__ == "__main__":
    print("1xBet Basketball Odds Scraper")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {BASE_URL}")
    print()

    # -------------------------------------------------------
    # OPTION 1: Discover all available basketball leagues
    # -------------------------------------------------------
    # leagues = scan_all_leagues()

    # -------------------------------------------------------
    # OPTION 2: Get games for a specific league
    # -------------------------------------------------------
    # games = scan_league_games(24669, "Brazil NBB")

    # -------------------------------------------------------
    # OPTION 3: Get odds for a specific game ID
    # -------------------------------------------------------
    # odds = get_full_odds_for_game(702123673)

    # -------------------------------------------------------
    # OPTION 4: Full pipeline — games + odds for a league
    # -------------------------------------------------------
    # all_odds = scan_league_with_odds(24669, "Brazil NBB")
    # export_odds_to_json(all_odds, "brazil_nbb_odds.json")

    # -------------------------------------------------------
    # OPTION 5: Scan multiple leagues at once
    # -------------------------------------------------------
    # target_leagues = {
    #     "South Korea KBL": 118927,
    #     "Brazil NBB": 24669,
    #     "Indonesia IBL": 34445,
    # }
    # for name, lid in target_leagues.items():
    #     all_odds = scan_league_with_odds(lid, name)
    #     export_odds_to_json(all_odds, f"{name.replace(' ', '_')}_odds.json")

    # -------------------------------------------------------
    # Uncomment ONE option above to run it.
    # Start with Option 1 to see what's available.
    # -------------------------------------------------------
    scan_all_leagues()
    get_league_games(118927, "South Korea KBL")
    games = get_league_games(118927, "South Korea KBL")
    if games:
        get_game_odds(games[0]["game_id"], f"{games[0]['home_team']} vs {games[0]['away_team']}")