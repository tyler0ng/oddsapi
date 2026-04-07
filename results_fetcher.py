"""
Results fetcher — pulls final scores from API-Basketball
and matches them to games in our database.

The tricky part: 1xBet and API-Basketball use different team names
and different game IDs, so we match on:
  1. Same date (within a day)
  2. Fuzzy team name matching

Usage:
  python results_fetcher.py                # fetch results for all unmatched games
  python results_fetcher.py --test         # test API connection
  python results_fetcher.py --leagues      # list available leagues on API-Basketball
"""

import re
import sys
import time
import requests
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from config import API_BASKETBALL_KEY, API_BASKETBALL_BASE
from database import (
    init_db, get_db, get_games_without_results, store_game_result, get_db_stats
)


# ============================================================
# API-BASKETBALL CLIENT
# ============================================================

API_HEADERS = {
    "x-apisports-key": API_BASKETBALL_KEY,
}


def api_basketball_request(endpoint, params=None):
    """Make a request to API-Basketball."""
    url = f"{API_BASKETBALL_BASE}/{endpoint}"
    try:
        response = requests.get(url, params=params, headers=API_HEADERS, timeout=15)

        if response.status_code == 200:
            data = response.json()
            # API-Sports wraps everything in a response object
            remaining = data.get("results", 0)
            errors = data.get("errors", [])
            if errors and isinstance(errors, dict) and errors:
                print(f"  [API-BASKETBALL ERROR] {errors}")
                return None
            return data.get("response", [])

        elif response.status_code == 429:
            print("  [RATE LIMIT] API-Basketball rate limit hit, waiting...")
            time.sleep(60)
            return None

        else:
            print(f"  [API ERROR] Status {response.status_code}")
            return None

    except Exception as e:
        print(f"  [REQUEST ERROR] {e}")
        return None


def test_connection():
    """Test that the API key works."""
    print("[TEST] Testing API-Basketball connection...")
    result = api_basketball_request("timezone")
    if result is not None:
        print(f"  [OK] Connection works! Got {len(result)} timezones")
        return True
    else:
        print("  [FAIL] Could not connect. Check your API key.")
        return False


def fetch_basketball_leagues():
    """Get all available basketball leagues from API-Basketball."""
    results = api_basketball_request("leagues")
    if not results:
        return []

    leagues = []
    for item in results:
        leagues.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "country": item.get("country", {}).get("name", "Unknown"),
            "type": item.get("type"),
        })
    return leagues


def fetch_games_by_date(date_str):
    """
    Fetch all basketball games for a specific date.
    date_str: "YYYY-MM-DD"
    Returns list of finished games with scores.
    """
    results = api_basketball_request("games", params={"date": date_str})
    if not results:
        return []

    finished_games = []
    for game in results:
        status = game.get("status", {})
        # Only include finished games
        if status.get("short") not in ("FT", "AOT", "AP"):
            continue

        scores = game.get("scores", {})
        home_team_data = game.get("teams", {}).get("home", {})
        away_team_data = game.get("teams", {}).get("away", {})

        home_score = scores.get("home", {}).get("total")
        away_score = scores.get("away", {}).get("total")

        if home_score is None or away_score is None:
            continue

        # Parse quarter scores
        quarters = {}
        home_quarters = scores.get("home", {})
        away_quarters = scores.get("away", {})

        for q in range(1, 5):
            key = f"quarter_{q}"
            quarters[f"home_q{q}"] = home_quarters.get(key)
            quarters[f"away_q{q}"] = away_quarters.get(key)

        # Overtime
        quarters["home_ot"] = home_quarters.get("over_time")
        quarters["away_ot"] = away_quarters.get("over_time")

        finished_games.append({
            "api_game_id": game.get("id"),
            "home_team": home_team_data.get("name", "Unknown"),
            "away_team": away_team_data.get("name", "Unknown"),
            "home_score": int(home_score),
            "away_score": int(away_score),
            "quarters": quarters,
            "league_name": game.get("league", {}).get("name", "Unknown"),
            "league_country": game.get("league", {}).get("country", {}).get("name", "Unknown"),
            "status": status.get("long", "Finished"),
            "date": game.get("date"),
        })

    return finished_games


# ============================================================
# TEAM NAME MATCHING
# ============================================================

def normalize_name(name):
    """Normalize team name for comparison."""
    if not name:
        return ""
    # Lowercase, strip common suffixes/prefixes
    n = name.lower().strip()
    # Remove common suffixes that differ between sources
    for suffix in [" bc", " bk", " sk", " fc", " sc", " basket",
                   " basketball", " club", " team"]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def team_similarity(name1, name2):
    """
    Calculate similarity between two team names.
    Uses multiple strategies since 1xBet and API-Basketball
    often use different naming conventions.

    Examples:
      "Changwon LG Sakers" vs "LG Sakers" → high match
      "Philadelphia 76ers" vs "Philadelphia" → high match
    """
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    # Exact match after normalization
    if n1 == n2:
        return 1.0

    # One name contains the other
    if n1 in n2 or n2 in n1:
        return 0.85

    # Check if any significant word overlaps
    words1 = set(n1.split())
    words2 = set(n2.split())
    common = words1 & words2
    if common:
        # Weight by how much of the name the common words cover
        overlap = len(common) / max(len(words1), len(words2))
        if overlap >= 0.5:
            return 0.7 + (overlap * 0.2)

    # Fallback: sequence matcher
    return SequenceMatcher(None, n1, n2).ratio()


# ============================================================
# FILTERING
# ============================================================

# Keywords that indicate a 1xBet entry is NOT a real game
JUNK_KEYWORDS = [
    "winner", "specials", "enhanced", "season", "championship",
    "mvp", "top scorer", "outright", "futures", "special",
    "(points)", "1st half winner", "2nd half winner",
    "3x3", "u21", "u23", "u19",  # youth/3x3 usually can't match
]


def is_junk_game(game):
    """
    Filter out 1xBet entries that aren't real matchups.
    These include futures, specials, winner markets, and youth games
    that won't have matching API-Basketball results.
    """
    home = (game["home_team"] or "").lower()
    away = (game["away_team"] or "").lower()
    league = (game["league_name"] or "").lower()
    combined = f"{home} {away} {league}"

    # No away team = not a real game
    if not away or away.strip() == "":
        return True

    # Check for junk keywords
    for kw in JUNK_KEYWORDS:
        if kw in combined:
            return True

    return False


# ============================================================
# MATCHING
# ============================================================

def _api_game_unix_time(api_game):
    """Parse API-Basketball's ISO date field to a unix timestamp, or None."""
    date_str = api_game.get("date")
    if not date_str:
        return None
    try:
        # Handles "2026-03-30T19:00:00+00:00" and trailing "Z"
        s = date_str.replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except (ValueError, TypeError):
        return None


def match_game(our_game, api_games, threshold=0.70, max_time_gap_hours=12):
    """
    Try to match one of our tracked games to an API-Basketball result.
    Returns (best_match, score, swapped) — swapped=True means the API's
    home team is our away team (and vice versa), so the caller must swap
    the home/away scores before storing.

    When our_game has a start_time, API games more than max_time_gap_hours
    away are filtered out — prevents matching same-teams-different-day
    games (e.g. home-and-away legs of a double round-robin).
    If our_game has no start_time, falls back to name-only matching.
    """
    # Skip junk entries that aren't real games
    if is_junk_game(our_game):
        return None, 0, False

    best_match = None
    best_score = 0
    best_swapped = False

    our_home = our_game["home_team"]
    our_away = our_game["away_team"]
    # sqlite3.Row doesn't support .get(); access directly (column exists, may be NULL)
    our_start = our_game["start_time"] if "start_time" in our_game.keys() else None
    max_gap_s = max_time_gap_hours * 3600

    for api_game in api_games:
        # Time-proximity filter (skip only when both sides have a timestamp)
        if our_start:
            api_ts = _api_game_unix_time(api_game)
            if api_ts is not None and abs(api_ts - our_start) > max_gap_s:
                continue

        api_home = api_game["home_team"]
        api_away = api_game["away_team"]

        # Score = average of home and away name similarity
        home_sim = team_similarity(our_home, api_home)
        away_sim = team_similarity(our_away, api_away)
        avg_sim = (home_sim + away_sim) / 2

        # Also check if teams are swapped (home/away mismatch)
        swap_home_sim = team_similarity(our_home, api_away)
        swap_away_sim = team_similarity(our_away, api_home)
        swap_avg = (swap_home_sim + swap_away_sim) / 2

        # Track which orientation won
        if swap_avg > avg_sim:
            score, swapped = swap_avg, True
        else:
            score, swapped = avg_sim, False

        if score > best_score:
            best_score = score
            best_match = api_game
            best_swapped = swapped

    if best_score >= threshold:
        return best_match, best_score, best_swapped
    return None, 0, False


# ============================================================
# MAIN RESULTS PIPELINE
# ============================================================

def fetch_results_for_pending_games():
    """
    Main pipeline:
    1. Find games in our DB without results
    2. For each game's date, fetch API-Basketball results
    3. Match and store
    """
    print("\n[RESULTS] Checking for games without results...")

    with get_db() as conn:
        pending = get_games_without_results(conn)

    if not pending:
        print("  No pending games found")
        return 0

    print(f"  Found {len(pending)} games without results")

    # Group games by date to minimize API calls
    # We'll check yesterday and today (most games should be done within a day)
    dates_to_check = set()
    now = datetime.utcnow()

    # Check the last 3 days — covers games that just finished
    for days_back in range(0, 4):
        date = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
        dates_to_check.add(date)

    # Also add dates from game start_time if available
    for game in pending:
        if game["start_time"]:
            try:
                game_date = datetime.fromtimestamp(game["start_time"]).strftime("%Y-%m-%d")
                # Only check if the game date is in the past (game should be finished)
                if game_date <= now.strftime("%Y-%m-%d"):
                    dates_to_check.add(game_date)
            except (ValueError, OSError):
                pass

    print(f"  Checking dates: {sorted(dates_to_check)}")

    # Fetch all API results for these dates
    all_api_games = []
    for date_str in sorted(dates_to_check):
        print(f"\n  [API] Fetching results for {date_str}...")
        games = fetch_games_by_date(date_str)
        print(f"    Got {len(games)} finished games")
        all_api_games.extend(games)
        time.sleep(1)  # Be polite to API

    if not all_api_games:
        print("  No finished games found in API")
        return 0

    # Match our games to API results
    # Track which API games have been used to prevent one result matching multiple 1xBet entries
    matched = 0
    used_api_ids = set()

    with get_db() as conn:
        for game in pending:
            # Filter out remaining API games that haven't been matched yet
            available_api_games = [
                g for g in all_api_games if g["api_game_id"] not in used_api_ids
            ]

            result, score, swapped = match_game(game, available_api_games)

            if result:
                # When API's home is our away (swapped match), flip scores and
                # quarters so they align with our game's home/away labels.
                if swapped:
                    home_score = result["away_score"]
                    away_score = result["home_score"]
                    q_in = result["quarters"] or {}
                    quarters = {
                        "home_q1": q_in.get("away_q1"), "away_q1": q_in.get("home_q1"),
                        "home_q2": q_in.get("away_q2"), "away_q2": q_in.get("home_q2"),
                        "home_q3": q_in.get("away_q3"), "away_q3": q_in.get("home_q3"),
                        "home_q4": q_in.get("away_q4"), "away_q4": q_in.get("home_q4"),
                        "home_ot": q_in.get("away_ot"), "away_ot": q_in.get("home_ot"),
                    }
                else:
                    home_score = result["home_score"]
                    away_score = result["away_score"]
                    quarters = result["quarters"]

                stored = store_game_result(
                    conn,
                    game_id=game["id"],
                    home_score=home_score,
                    away_score=away_score,
                    quarters=quarters,
                    status=result["status"],
                    api_game_id=result["api_game_id"],
                )

                if stored:
                    matched += 1
                    used_api_ids.add(result["api_game_id"])
                    total = home_score + away_score
                    swap_note = " [home/away swapped]" if swapped else ""
                    print(f"  [MATCHED] {game['home_team']} vs {game['away_team']}{swap_note}")
                    print(f"    Score: {home_score}-{away_score} (total: {total})")
                    print(f"    Match confidence: {score:.0%}")

                    # Retag based on API-Basketball league info
                    api_league = (result.get("league_name") or "").lower()
                    our_league = game["league_name"] or ""
                    if "women" in api_league and "women" not in our_league.lower():
                        new_name = our_league + " Women"
                        conn.execute(
                            "UPDATE games SET league_name = ? WHERE id = ?",
                            (new_name, game["id"]),
                        )
                        print(f"    Retagged → {new_name}")
                    elif "women" not in api_league and "women" in our_league.lower():
                        # 1xBet mislabeled a men's game as women's — fix it
                        new_name = re.sub(r'\.?\s*Women$', '', our_league, flags=re.IGNORECASE).strip()
                        conn.execute(
                            "UPDATE games SET league_name = ? WHERE id = ?",
                            (new_name, game["id"]),
                        )
                        print(f"    Retagged (men's) → {new_name}")

    print(f"\n[RESULTS] Stored {matched} new results")
    return matched


# ============================================================
# RETAG WOMEN'S LEAGUES
# ============================================================

def retag_womens_leagues():
    """
    Retroactively check already-matched games against API-Basketball
    and fix men/women classification. Handles both directions:
    - Men's games mislabeled as Women's → strip "Women" suffix
    - Women's games missing the label → add "Women" suffix
    """
    print("\n[RETAG] Checking matched games for men/women classification...")

    with get_db() as conn:
        rows = conn.execute("""
            SELECT g.id, g.league_name, gr.api_game_id
            FROM games g
            JOIN game_results gr ON g.id = gr.game_id
            WHERE gr.api_game_id IS NOT NULL
        """).fetchall()

    if not rows:
        print("  No games to check")
        return 0

    print(f"  Checking {len(rows)} games against API-Basketball...")

    retagged = 0
    with get_db() as conn:
        for row in rows:
            api_game_id = row["api_game_id"]

            result = api_basketball_request("games", params={"id": str(api_game_id)})
            if not result or len(result) == 0:
                continue

            game_data = result[0]
            api_league = game_data.get("league", {}).get("name", "")
            our_league = row["league_name"] or ""
            is_api_women = "women" in api_league.lower()
            is_our_women = "women" in our_league.lower()

            if is_api_women and not is_our_women:
                new_name = our_league + " Women"
                conn.execute(
                    "UPDATE games SET league_name = ? WHERE id = ?",
                    (new_name, row["id"]),
                )
                retagged += 1
                print(f"  [RETAGGED] {our_league} → {new_name} (api_game_id={api_game_id})")
            elif not is_api_women and is_our_women:
                new_name = re.sub(r'\.?\s*Women$', '', our_league, flags=re.IGNORECASE).strip()
                conn.execute(
                    "UPDATE games SET league_name = ? WHERE id = ?",
                    (new_name, row["id"]),
                )
                retagged += 1
                print(f"  [RETAGGED] {our_league} → {new_name} (api_game_id={api_game_id})")

            time.sleep(0.5)  # Rate limit

    print(f"\n[RETAG] Updated {retagged} games")
    return retagged


# ============================================================
# ENTRY POINTS
# ============================================================

if __name__ == "__main__":
    init_db()

    if "--test" in sys.argv:
        test_connection()

    elif "--leagues" in sys.argv:
        leagues = fetch_basketball_leagues()
        print(f"\nFound {len(leagues)} leagues:\n")
        for lg in sorted(leagues, key=lambda x: x["country"]):
            print(f"  {lg['country']:<25} {lg['name']:<40} ID: {lg['id']}")

    elif "--retag" in sys.argv:
        retag_womens_leagues()

    else:
        fetch_results_for_pending_games()
        with get_db() as conn:
            stats = get_db_stats(conn)
        print(f"\n[DB] {stats}")
