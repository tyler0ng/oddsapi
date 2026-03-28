"""
Scheduler — the main loop that runs 24/7.

Workflow per cycle:
  1. Check which target leagues have games
  2. For each game, fetch odds
  3. Store in database (only if odds changed)
  4. Log the run
  5. Sleep until next cycle

Usage:
  python scheduler.py              # run the loop
  python scheduler.py --once       # single run then exit (for testing)
  python scheduler.py --status     # show DB stats and recent runs
"""

import sys
import time
from datetime import datetime
from scraper import fetch_all_leagues, fetch_league_games, fetch_and_parse_game_odds
from database import init_db, get_db, store_game_odds, start_run, finish_run, log_league_run, get_db_stats, get_run_history
from config import (
    POLL_INTERVAL_DEFAULT,
    POLL_INTERVAL_APPROACHING,
    POLL_INTERVAL_IMMINENT,
    REQUEST_DELAY,
    EXCLUDED_LEAGUES,
)
from notifier import notify_startup, notify_error, notify_warning, notify_cycle_summary


# ============================================================
# SINGLE SCRAPE CYCLE
# ============================================================

def run_cycle(run_id=None):
    """
    Execute one full scrape cycle across all target leagues.
    Returns: (leagues_checked, games_found, odds_stored, errors)
    """
    leagues_checked = 0
    games_found = 0
    odds_stored = 0
    errors = 0

    # Step 1: discover ALL basketball leagues with active games
    print("\n[DISCOVER] Fetching all basketball leagues...")
    available = fetch_all_leagues()
    print(f"[DISCOVER] Found {len(available)} leagues with games")

    if len(available) == 0:
        notify_warning("0 leagues found — check BASE_URL/session")

    # Step 2: scrape every single one — no filtering
    for league in available:
        league_name = league["name"]
        league_id = league["id"]

        if league["game_count"] == 0:
            continue  # listed but no games, skip

        if league_name in EXCLUDED_LEAGUES:
            print(f"  [SKIP] {league_name} (excluded)")
            continue

        leagues_checked += 1
        print(f"\n[LEAGUE] {league_name} (ID: {league_id}) — {league['game_count']} games")

        # Fetch game list
        games = fetch_league_games(league_id, league_name)
        if not games:
            print(f"  No games found")
            if run_id:
                with get_db() as conn:
                    log_league_run(conn, run_id, league_name, league_id, 0, 0, 0)
            continue

        league_games = len(games)
        league_odds = 0
        league_errors = 0

        games_found += league_games
        print(f"  Found {league_games} games")

        # Fetch odds for each game and store
        with get_db() as conn:
            for game in games:
                gid = game["game_id"]
                matchup = f"{game['home_team']} vs {game['away_team']}"

                try:
                    parsed = fetch_and_parse_game_odds(gid)
                    if not parsed:
                        print(f"  [SKIP] No odds for {matchup}")
                        continue

                    # Determine if this is a pre-match or live snapshot
                    now_ts = int(time.time())
                    start_ts = parsed.get("start_ts") or game.get("start_time")
                    period = parsed.get("period", "Full Game")

                    is_prematch = 1
                    if start_ts and now_ts >= start_ts:
                        is_prematch = 0
                    if period != "Full Game":
                        is_prematch = 0  # period-specific = live

                    # Derive game status
                    if is_prematch:
                        game_status = "upcoming"
                    else:
                        game_status = "live"

                    new_snapshots = store_game_odds(
                        conn,
                        ext_game_id=gid,
                        league_name=league_name,
                        league_id=league_id,
                        home_team=game["home_team"],
                        away_team=game["away_team"],
                        parsed_odds=parsed,
                        start_time=start_ts or game.get("start_time"),
                        is_prematch=is_prematch,
                        game_status=game_status,
                    )

                    league_odds += new_snapshots
                    status = f"{new_snapshots} new" if new_snapshots else "no changes"
                    print(f"  [OK] {matchup} — {parsed['total_markets']} markets, {status}")

                except Exception as e:
                    league_errors += 1
                    print(f"  [ERROR] {matchup}: {e}")
                    notify_error(f"{league_name} — {matchup}", e)

                # Don't hammer the API
                time.sleep(REQUEST_DELAY)

            # Log per-league stats for this run
            if run_id:
                log_league_run(conn, run_id, league_name, league_id, league_games, league_odds, league_errors)

        odds_stored += league_odds
        errors += league_errors

    return leagues_checked, games_found, odds_stored, errors


# ============================================================
# MAIN LOOP
# ============================================================

def calculate_sleep_interval():
    """
    Determine how long to sleep based on upcoming game times.
    If games are imminent, poll faster.
    """
    # For now, use default. Phase 2 improvement: check game start times
    # and adjust interval dynamically.
    #
    # TODO: query DB for nearest game start_time and adjust:
    #   < 30 min away  → POLL_INTERVAL_IMMINENT  (60s)
    #   < 2 hours away → POLL_INTERVAL_APPROACHING (120s)
    #   otherwise      → POLL_INTERVAL_DEFAULT (300s)
    return POLL_INTERVAL_DEFAULT


def main_loop():
    """Run the scraper in a continuous loop."""
    print("=" * 60)
    print("  1xBet Basketball Odds Tracker")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Tracking: ALL basketball leagues (auto-discovery)")
    print(f"  Poll interval: {POLL_INTERVAL_DEFAULT}s")
    print("=" * 60)

    # Initialize database
    init_db()
    notify_startup()

    cycle_count = 0

    while True:
        cycle_count += 1
        cycle_start = time.time()

        print(f"\n{'━' * 60}")
        print(f"  CYCLE #{cycle_count} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'━' * 60}")

        # Log the run
        with get_db() as conn:
            run_id = start_run(conn)

        try:
            leagues_checked, games_found, odds_stored, errors = run_cycle(run_id)

            with get_db() as conn:
                finish_run(conn, run_id, leagues_checked, games_found, odds_stored, errors)

            elapsed = time.time() - cycle_start
            print(f"\n[DONE] Cycle #{cycle_count}: "
                  f"{leagues_checked} leagues, {games_found} games, "
                  f"{odds_stored} new odds, {errors} errors "
                  f"({elapsed:.1f}s)")

            notify_cycle_summary(leagues_checked, games_found, odds_stored, errors)

        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user")
            with get_db() as conn:
                finish_run(conn, run_id, 0, 0, 0, 0, status="interrupted")
            break

        except Exception as e:
            print(f"\n[CRITICAL ERROR] {e}")
            notify_error("Critical — main loop", e)
            with get_db() as conn:
                finish_run(conn, run_id, 0, 0, 0, 1, status="failed")
            # Don't crash — wait and try again
            import traceback
            traceback.print_exc()

        # Sleep until next cycle
        interval = calculate_sleep_interval()
        print(f"\n[SLEEP] Next cycle in {interval}s...")

        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[STOP] Interrupted during sleep")
            break

    # Final stats
    with get_db() as conn:
        stats = get_db_stats(conn)
    print(f"\n[FINAL] {stats}")


# ============================================================
# ONE-OFF COMMANDS
# ============================================================

def run_once():
    """Single scrape cycle, then exit. Good for testing."""
    print("[MODE] Single run")
    init_db()

    with get_db() as conn:
        run_id = start_run(conn)

    leagues_checked, games_found, odds_stored, errors = run_cycle(run_id)

    with get_db() as conn:
        finish_run(conn, run_id, leagues_checked, games_found, odds_stored, errors)
        stats = get_db_stats(conn)

    print(f"\n[RESULT] {leagues_checked} leagues, {games_found} games, "
          f"{odds_stored} new snapshots, {errors} errors")
    print(f"[DB] {stats}")


def show_status():
    """Show database stats and recent runs."""
    init_db()

    with get_db() as conn:
        stats = get_db_stats(conn)
        runs = get_run_history(conn, limit=10)

    print("\n[DATABASE STATS]")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    print(f"\n[RECENT RUNS] (last {len(runs)})")
    for r in runs:
        print(f"  {r['started_at']} | {r['status']:>10} | "
              f"leagues={r['leagues_checked']} games={r['games_found']} "
              f"odds={r['odds_stored']} errors={r['errors']}")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    if "--once" in sys.argv:
        run_once()
    elif "--status" in sys.argv:
        show_status()
    else:
        main_loop()
