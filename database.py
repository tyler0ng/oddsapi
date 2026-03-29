"""
Database layer for odds tracking.
Uses SQLite for simplicity — swap to PostgreSQL later if needed.
"""

import sqlite3
import time
from datetime import datetime
from contextlib import contextmanager
from config import DB_PATH


# ============================================================
# CONNECTION MANAGEMENT
# ============================================================

@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # dict-like access
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrent read/write
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# SCHEMA
# ============================================================

def migrate_db():
    """Add new columns to existing tables (safe to re-run)."""
    migrations = [
        ("games", "game_status", "TEXT DEFAULT 'upcoming'"),
        ("odds_snapshots", "is_prematch", "INTEGER DEFAULT 1"),
        ("game_results", "regulation_total", "INTEGER"),
    ]
    with get_db() as conn:
        for table, column, col_type in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
                print(f"[MIGRATE] Added {table}.{column}")
            except Exception as e:
                if "duplicate column" in str(e).lower():
                    pass  # already exists, fine
                else:
                    raise

        # Backfill: mark games with results as 'finished'
        conn.execute("""
            UPDATE games SET game_status = 'finished'
            WHERE id IN (SELECT game_id FROM game_results)
              AND game_status != 'finished'
        """)

        # Backfill: mark snapshots taken after game start_time as live
        conn.execute("""
            UPDATE odds_snapshots SET is_prematch = 0
            WHERE is_prematch = 1
              AND game_id IN (
                  SELECT g.id FROM games g
                  WHERE g.start_time IS NOT NULL
              )
              AND CAST(strftime('%s', scraped_at) AS INTEGER) >= (
                  SELECT g.start_time FROM games g
                  WHERE g.id = odds_snapshots.game_id
              )
        """)


def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        conn.executescript("""
            -- Tracks unique games
            CREATE TABLE IF NOT EXISTS games (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ext_game_id     INTEGER UNIQUE NOT NULL,  -- 1xBet's game ID
                league_name     TEXT NOT NULL,
                league_id       INTEGER NOT NULL,
                home_team       TEXT NOT NULL,
                away_team       TEXT NOT NULL,
                start_time      INTEGER,                  -- unix timestamp
                game_status     TEXT DEFAULT 'upcoming',  -- upcoming, live, finished
                first_seen      TEXT DEFAULT (datetime('now')),
                last_updated    TEXT DEFAULT (datetime('now'))
            );

            -- Every odds snapshot for every market line
            CREATE TABLE IF NOT EXISTS odds_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id         INTEGER NOT NULL REFERENCES games(id),
                market_group    TEXT NOT NULL,       -- e.g. "Total", "Handicap", "Moneyline"
                market_type     INTEGER NOT NULL,    -- 1xBet type code (9=Over, 10=Under, etc.)
                market_label    TEXT NOT NULL,        -- human readable: "Over", "Home Handicap"
                line            REAL,                -- the parameter (e.g. 151.5)
                odds            REAL NOT NULL,       -- decimal odds
                odds_display    TEXT,                -- as shown on site
                is_main_line    INTEGER DEFAULT 0,   -- 1 if center/main line
                is_prematch     INTEGER DEFAULT 1,   -- 1 = pre-match, 0 = live/in-play
                scraped_at      TEXT DEFAULT (datetime('now'))
            );

            -- Tracks line movements: the main line + odds over time per game/market
            -- This is a VIEW, not a table — auto-computed from snapshots
            CREATE VIEW IF NOT EXISTS line_movements AS
            SELECT
                g.home_team,
                g.away_team,
                g.league_name,
                os.market_group,
                os.market_label,
                os.line,
                os.odds,
                os.scraped_at,
                os.is_main_line
            FROM odds_snapshots os
            JOIN games g ON g.id = os.game_id
            WHERE os.is_main_line = 1
            ORDER BY g.ext_game_id, os.market_group, os.market_label, os.scraped_at;

            -- Index for fast lookups
            CREATE INDEX IF NOT EXISTS idx_snapshots_game
                ON odds_snapshots(game_id, market_group, scraped_at);

            CREATE INDEX IF NOT EXISTS idx_snapshots_time
                ON odds_snapshots(scraped_at);

            CREATE INDEX IF NOT EXISTS idx_games_ext
                ON games(ext_game_id);

            -- Tracks scraper runs for monitoring health
            CREATE TABLE IF NOT EXISTS scraper_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at      TEXT DEFAULT (datetime('now')),
                finished_at     TEXT,
                leagues_checked INTEGER DEFAULT 0,
                games_found     INTEGER DEFAULT 0,
                odds_stored     INTEGER DEFAULT 0,
                errors          INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'running'  -- running, completed, failed
            );

            -- Per-league breakdown within each scraper run
            CREATE TABLE IF NOT EXISTS scraper_run_leagues (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id          INTEGER NOT NULL REFERENCES scraper_runs(id),
                league_name     TEXT NOT NULL,
                league_id       INTEGER NOT NULL,
                games_found     INTEGER DEFAULT 0,
                odds_stored     INTEGER DEFAULT 0,
                errors          INTEGER DEFAULT 0,
                scraped_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_run_leagues_run
                ON scraper_run_leagues(run_id);

            CREATE INDEX IF NOT EXISTS idx_run_leagues_league
                ON scraper_run_leagues(league_name);

            -- Game results from API-Basketball
            CREATE TABLE IF NOT EXISTS game_results (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id         INTEGER NOT NULL REFERENCES games(id),
                home_score      INTEGER NOT NULL,
                away_score      INTEGER NOT NULL,
                total_points    INTEGER NOT NULL,       -- home + away (for total market comparison)
                regulation_total INTEGER,               -- total without OT (for analysis)
                home_q1         INTEGER,
                away_q1         INTEGER,
                home_q2         INTEGER,
                away_q2         INTEGER,
                home_q3         INTEGER,
                away_q3         INTEGER,
                home_q4         INTEGER,
                away_q4         INTEGER,
                home_ot         INTEGER,                -- overtime points (if any)
                away_ot         INTEGER,
                status          TEXT DEFAULT 'final',    -- final, OT, etc.
                api_game_id     INTEGER,                 -- API-Basketball's game ID (for cross-ref)
                fetched_at      TEXT DEFAULT (datetime('now')),
                UNIQUE(game_id)                          -- one result per game
            );

            CREATE INDEX IF NOT EXISTS idx_results_game
                ON game_results(game_id);
        """)

    migrate_db()
    print("[DB] Database initialized.")


# ============================================================
# WRITE OPERATIONS
# ============================================================

def upsert_game(conn, ext_game_id, league_name, league_id, home_team, away_team, start_time=None, game_status=None):
    """
    Insert a game if new, or update last_updated if it already exists.
    Fills in start_time if previously null. Advances game_status forward only.
    Returns the internal game ID.
    """
    # Try to find existing
    row = conn.execute(
        "SELECT id, game_status FROM games WHERE ext_game_id = ?", (ext_game_id,)
    ).fetchone()

    if row:
        # Determine new status: only advance forward (upcoming → live → finished)
        status_order = {'upcoming': 0, 'live': 1, 'finished': 2}
        current = row["game_status"] or 'upcoming'
        new_status = current
        if game_status and status_order.get(game_status, 0) > status_order.get(current, 0):
            new_status = game_status

        conn.execute(
            """UPDATE games
               SET last_updated = datetime('now'),
                   start_time = COALESCE(?, start_time),
                   game_status = ?
               WHERE id = ?""",
            (start_time, new_status, row["id"])
        )
        return row["id"]
    else:
        cursor = conn.execute(
            """INSERT INTO games (ext_game_id, league_name, league_id, home_team, away_team, start_time, game_status)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (ext_game_id, league_name, league_id, home_team, away_team, start_time, game_status or 'upcoming')
        )
        return cursor.lastrowid


def store_odds_snapshot(conn, game_id, market_group, market_type, market_label, line, odds, odds_display, is_main_line, is_prematch=1):
    """
    Store a single odds snapshot.
    Skips if identical odds were already stored in the last snapshot for this game/market/line.
    This avoids bloating the DB with duplicate rows when nothing has changed.
    """
    # Check if the last snapshot for this exact market+line is the same
    last = conn.execute(
        """SELECT odds FROM odds_snapshots
           WHERE game_id = ? AND market_group = ? AND market_type = ? AND line IS ?
           ORDER BY scraped_at DESC LIMIT 1""",
        (game_id, market_group, market_type, line)
    ).fetchone()

    if last and last["odds"] == odds:
        return False  # no change, skip

    conn.execute(
        """INSERT INTO odds_snapshots
           (game_id, market_group, market_type, market_label, line, odds, odds_display, is_main_line, is_prematch)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (game_id, market_group, market_type, market_label, line, odds, odds_display, int(is_main_line), int(is_prematch))
    )
    return True  # new snapshot stored


def store_game_odds(conn, ext_game_id, league_name, league_id, home_team, away_team, parsed_odds, start_time=None, is_prematch=1, game_status=None):
    """
    High-level: store a full set of parsed odds for one game.
    Returns count of new snapshots stored.
    """
    game_id = upsert_game(conn, ext_game_id, league_name, league_id, home_team, away_team, start_time, game_status)

    new_count = 0
    for market_group, entries in parsed_odds.get("markets", {}).items():
        for entry in entries:
            stored = store_odds_snapshot(
                conn,
                game_id=game_id,
                market_group=market_group,
                market_type=entry.get("type", 0),
                market_label=entry.get("type_name", "Unknown"),
                line=entry.get("line"),
                odds=entry.get("odds", 0),
                odds_display=entry.get("odds_display", ""),
                is_main_line=entry.get("is_main_line", False),
                is_prematch=is_prematch,
            )
            if stored:
                new_count += 1

    return new_count


# ============================================================
# SCRAPER RUN TRACKING
# ============================================================

def start_run(conn):
    """Log the start of a scraper run. Returns run ID."""
    cursor = conn.execute("INSERT INTO scraper_runs DEFAULT VALUES")
    return cursor.lastrowid


def log_league_run(conn, run_id, league_name, league_id, games_found, odds_stored, errors):
    """Log per-league stats within a scraper run."""
    conn.execute(
        """INSERT INTO scraper_run_leagues (run_id, league_name, league_id, games_found, odds_stored, errors)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (run_id, league_name, league_id, games_found, odds_stored, errors)
    )


def finish_run(conn, run_id, leagues_checked, games_found, odds_stored, errors, status="completed"):
    """Log the end of a scraper run."""
    conn.execute(
        """UPDATE scraper_runs
           SET finished_at = datetime('now'),
               leagues_checked = ?,
               games_found = ?,
               odds_stored = ?,
               errors = ?,
               status = ?
           WHERE id = ?""",
        (leagues_checked, games_found, odds_stored, errors, status, run_id)
    )


# ============================================================
# READ OPERATIONS (for EV model + monitoring)
# ============================================================

def get_latest_odds(conn, ext_game_id, market_group=None):
    """Get the most recent odds snapshot for each market/line of a game."""
    query = """
        SELECT os.*, g.home_team, g.away_team, g.league_name
        FROM odds_snapshots os
        JOIN games g ON g.id = os.game_id
        WHERE g.ext_game_id = ?
    """
    params = [ext_game_id]

    if market_group:
        query += " AND os.market_group = ?"
        params.append(market_group)

    query += """
        AND os.scraped_at = (
            SELECT MAX(os2.scraped_at)
            FROM odds_snapshots os2
            WHERE os2.game_id = os.game_id
              AND os2.market_group = os.market_group
              AND os2.market_type = os.market_type
              AND os2.line IS os.line
        )
        ORDER BY os.market_group, os.line
    """
    return conn.execute(query, params).fetchall()


def get_line_history(conn, ext_game_id, market_group, market_label=None):
    """
    Get the full history of odds changes for a game's market.
    Useful for spotting sharp line movements.
    """
    query = """
        SELECT os.line, os.odds, os.odds_display, os.is_main_line, os.scraped_at
        FROM odds_snapshots os
        JOIN games g ON g.id = os.game_id
        WHERE g.ext_game_id = ? AND os.market_group = ?
    """
    params = [ext_game_id, market_group]

    if market_label:
        query += " AND os.market_label = ?"
        params.append(market_label)

    query += " ORDER BY os.scraped_at ASC"
    return conn.execute(query, params).fetchall()


def get_upcoming_games(conn, league_name=None):
    """Get all games we're tracking."""
    query = "SELECT * FROM games"
    params = []
    if league_name:
        query += " WHERE league_name = ?"
        params.append(league_name)
    query += " ORDER BY start_time ASC"
    return conn.execute(query, params).fetchall()


def get_run_history(conn, limit=20):
    """Get recent scraper runs for monitoring."""
    return conn.execute(
        "SELECT * FROM scraper_runs ORDER BY started_at DESC LIMIT ?",
        (limit,)
    ).fetchall()


def get_db_stats(conn):
    """Quick health check stats."""
    stats = {}
    stats["total_games"] = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    stats["total_snapshots"] = conn.execute("SELECT COUNT(*) FROM odds_snapshots").fetchone()[0]
    stats["total_runs"] = conn.execute("SELECT COUNT(*) FROM scraper_runs").fetchone()[0]
    stats["total_results"] = conn.execute("SELECT COUNT(*) FROM game_results").fetchone()[0]

    latest = conn.execute(
        "SELECT MAX(scraped_at) FROM odds_snapshots"
    ).fetchone()[0]
    stats["last_snapshot"] = latest

    games_with_results = conn.execute(
        "SELECT COUNT(*) FROM games g JOIN game_results gr ON g.id = gr.game_id"
    ).fetchone()[0]
    stats["games_with_results"] = games_with_results

    return stats


# ============================================================
# RESULTS OPERATIONS
# ============================================================

def store_game_result(conn, game_id, home_score, away_score, quarters=None, status="final", api_game_id=None):
    """
    Store the final result for a game.
    quarters = {"home_q1": 25, "away_q1": 30, ...} (optional)
    Returns True if stored, False if already exists.
    """
    # Check if result already stored
    existing = conn.execute(
        "SELECT id FROM game_results WHERE game_id = ?", (game_id,)
    ).fetchone()

    if existing:
        return False

    q = quarters or {}
    total_points = home_score + away_score
    home_ot = q.get("home_ot") or 0
    away_ot = q.get("away_ot") or 0
    regulation_total = total_points - home_ot - away_ot

    conn.execute(
        """INSERT INTO game_results
           (game_id, home_score, away_score, total_points, regulation_total,
            home_q1, away_q1, home_q2, away_q2, home_q3, away_q3, home_q4, away_q4,
            home_ot, away_ot, status, api_game_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (game_id, home_score, away_score, total_points, regulation_total,
         q.get("home_q1"), q.get("away_q1"),
         q.get("home_q2"), q.get("away_q2"),
         q.get("home_q3"), q.get("away_q3"),
         q.get("home_q4"), q.get("away_q4"),
         q.get("home_ot"), q.get("away_ot"),
         status, api_game_id)
    )
    return True


def get_games_without_results(conn):
    """
    Find games that we have odds for but no result yet.
    These are candidates for results fetching.
    """
    return conn.execute("""
        SELECT g.*
        FROM games g
        LEFT JOIN game_results gr ON g.id = gr.game_id
        WHERE gr.id IS NULL
        ORDER BY g.start_time ASC
    """).fetchall()


def get_closing_line_vs_result(conn, league_name=None):
    """
    THE MONEY QUERY: Compare the closing line (last odds before game)
    against the actual result for each game.

    Returns rows with: game info, closing total line, actual total, delta,
    and whether Over or Under would have won.
    """
    query = """
        SELECT
            g.home_team,
            g.away_team,
            g.league_name,
            g.ext_game_id,
            gr.home_score,
            gr.away_score,
            gr.total_points,
            closing.line         AS closing_total_line,
            closing.odds         AS closing_over_odds,
            gr.total_points - closing.line AS delta,
            CASE
                WHEN gr.total_points > closing.line THEN 'OVER'
                WHEN gr.total_points < closing.line THEN 'UNDER'
                ELSE 'PUSH'
            END AS result,
            gr.status
        FROM game_results gr
        JOIN games g ON g.id = gr.game_id
        -- Get the closing main-line Total Over
        -- Pick the line with odds closest to 1.90 (even money) at the latest timestamp,
        -- since 1xBet's isCenter flag is unreliable.
        LEFT JOIN (
            SELECT game_id, line, odds FROM (
                SELECT os.game_id, os.line, os.odds,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at DESC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Total'
                  AND os.market_type = 9
            ) WHERE rn = 1
        ) closing ON closing.game_id = gr.game_id
    """
    params = []
    if league_name:
        query += " WHERE g.league_name = ?"
        params.append(league_name)

    query += " ORDER BY gr.fetched_at DESC"
    return conn.execute(query, params).fetchall()


def get_closing_line_handicap_vs_result(conn, league_name=None):
    """
    Compare closing handicap line vs actual margin.
    """
    query = """
        SELECT
            g.home_team,
            g.away_team,
            g.league_name,
            gr.home_score,
            gr.away_score,
            (gr.home_score - gr.away_score) AS actual_margin,
            closing.line                     AS closing_handicap,
            closing.odds                     AS closing_odds,
            CASE
                WHEN (gr.home_score - gr.away_score) + closing.line > 0 THEN 'HOME COVERS'
                WHEN (gr.home_score - gr.away_score) + closing.line < 0 THEN 'AWAY COVERS'
                ELSE 'PUSH'
            END AS result
        FROM game_results gr
        JOIN games g ON g.id = gr.game_id
        LEFT JOIN (
            SELECT game_id, line, odds FROM (
                SELECT os.game_id, os.line, os.odds,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at DESC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Handicap'
                  AND os.market_type = 7
            ) WHERE rn = 1
        ) closing ON closing.game_id = gr.game_id
    """
    params = []
    if league_name:
        query += " WHERE g.league_name = ?"
        params.append(league_name)

    query += " ORDER BY gr.fetched_at DESC"
    return conn.execute(query, params).fetchall()


def get_closing_team_totals_vs_result(conn, league_name=None):
    """
    Compare closing team total line vs actual team score.
    Returns rows for both home and away team totals.
    """
    query = """
        SELECT
            g.home_team,
            g.away_team,
            g.league_name,
            g.ext_game_id,
            'Home' AS side,
            g.home_team AS team,
            gr.home_score AS actual_score,
            gr.total_points,
            closing.line AS closing_line,
            closing.odds AS closing_over_odds,
            gr.home_score - closing.line AS delta,
            CASE
                WHEN gr.home_score > closing.line THEN 'OVER'
                WHEN gr.home_score < closing.line THEN 'UNDER'
                ELSE 'PUSH'
            END AS result,
            gr.status
        FROM game_results gr
        JOIN games g ON g.id = gr.game_id
        LEFT JOIN (
            SELECT game_id, line, odds FROM (
                SELECT os.game_id, os.line, os.odds,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at DESC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Home Total'
                  AND os.market_type = 11
            ) WHERE rn = 1
        ) closing ON closing.game_id = gr.game_id
        WHERE closing.line IS NOT NULL
    """
    params = []
    if league_name:
        query += " AND g.league_name = ?"
        params.append(league_name)

    query += """
        UNION ALL
        SELECT
            g.home_team,
            g.away_team,
            g.league_name,
            g.ext_game_id,
            'Away' AS side,
            g.away_team AS team,
            gr.away_score AS actual_score,
            gr.total_points,
            closing.line AS closing_line,
            closing.odds AS closing_over_odds,
            gr.away_score - closing.line AS delta,
            CASE
                WHEN gr.away_score > closing.line THEN 'OVER'
                WHEN gr.away_score < closing.line THEN 'UNDER'
                ELSE 'PUSH'
            END AS result,
            gr.status
        FROM game_results gr
        JOIN games g ON g.id = gr.game_id
        LEFT JOIN (
            SELECT game_id, line, odds FROM (
                SELECT os.game_id, os.line, os.odds,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at DESC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Away Total'
                  AND os.market_type = 13
            ) WHERE rn = 1
        ) closing ON closing.game_id = gr.game_id
        WHERE closing.line IS NOT NULL
    """
    if league_name:
        query += " AND g.league_name = ?"
        params.append(league_name)

    query += " ORDER BY league_name, ext_game_id, side"
    return conn.execute(query, params).fetchall()


def get_clv_analysis(conn, league_name=None):
    """
    Closing Line Value: compare opening line (first prematch snapshot) vs closing
    line (last prematch snapshot) vs actual result. Shows whether early lines have edge.
    """
    query = """
        SELECT
            g.home_team,
            g.away_team,
            g.league_name,
            g.ext_game_id,
            opening.line  AS opening_line,
            opening.odds  AS opening_odds,
            closing.line  AS closing_line,
            closing.odds  AS closing_odds,
            closing.line - opening.line AS line_move,
            gr.total_points,
            gr.total_points - opening.line AS delta_vs_open,
            gr.total_points - closing.line AS delta_vs_close,
            CASE
                WHEN gr.total_points > opening.line THEN 'OVER'
                WHEN gr.total_points < opening.line THEN 'UNDER'
                ELSE 'PUSH'
            END AS result_vs_open,
            CASE
                WHEN gr.total_points > closing.line THEN 'OVER'
                WHEN gr.total_points < closing.line THEN 'UNDER'
                ELSE 'PUSH'
            END AS result_vs_close
        FROM game_results gr
        JOIN games g ON g.id = gr.game_id
        LEFT JOIN (
            SELECT game_id, line, odds FROM (
                SELECT os.game_id, os.line, os.odds,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at ASC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Total'
                  AND os.market_type = 9
            ) WHERE rn = 1
        ) opening ON opening.game_id = gr.game_id
        LEFT JOIN (
            SELECT game_id, line, odds FROM (
                SELECT os.game_id, os.line, os.odds,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at DESC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Total'
                  AND os.market_type = 9
            ) WHERE rn = 1
        ) closing ON closing.game_id = gr.game_id
        WHERE opening.line IS NOT NULL AND closing.line IS NOT NULL
    """
    params = []
    if league_name:
        query += " AND g.league_name = ?"
        params.append(league_name)

    query += " ORDER BY ABS(closing.line - opening.line) DESC"
    return conn.execute(query, params).fetchall()


def get_league_ou_bias(conn):
    """
    Over/under bias by league: historical over rate, avg delta,
    avg closing line vs avg actual total. Leagues that consistently
    go over suggest the book is setting lines too low.
    """
    return conn.execute("""
        SELECT
            g.league_name,
            COUNT(*) AS total_games,
            SUM(CASE WHEN gr.total_points > closing.line THEN 1 ELSE 0 END) AS overs,
            SUM(CASE WHEN gr.total_points < closing.line THEN 1 ELSE 0 END) AS unders,
            SUM(CASE WHEN gr.total_points = closing.line THEN 1 ELSE 0 END) AS pushes,
            ROUND(AVG(gr.total_points - closing.line), 1) AS avg_delta,
            ROUND(AVG(ABS(gr.total_points - closing.line)), 1) AS avg_abs_delta,
            ROUND(AVG(closing.line), 1) AS avg_closing_line,
            ROUND(AVG(CAST(gr.total_points AS REAL)), 1) AS avg_actual_total
        FROM game_results gr
        JOIN games g ON g.id = gr.game_id
        LEFT JOIN (
            SELECT game_id, line FROM (
                SELECT os.game_id, os.line,
                       ROW_NUMBER() OVER (PARTITION BY os.game_id
                           ORDER BY os.scraped_at DESC, ABS(os.odds - 1.90) ASC) AS rn
                FROM odds_snapshots os
                WHERE os.market_group = 'Total'
                  AND os.market_type = 9
            ) WHERE rn = 1
        ) closing ON closing.game_id = gr.game_id
        WHERE closing.line IS NOT NULL
        GROUP BY g.league_name
        HAVING total_games >= 3
        ORDER BY total_games DESC
    """).fetchall()


def get_hours_before_tipoff_patterns(conn, league_name=None):
    """
    Line movement activity bucketed by hours before game start.
    Uses window functions to detect actual line changes.
    """
    query = """
        WITH line_changes AS (
            SELECT
                os.game_id,
                g.league_name,
                os.line,
                os.odds,
                LAG(os.line) OVER (PARTITION BY os.game_id ORDER BY os.scraped_at) AS prev_line,
                CAST(
                    (g.start_time - CAST(strftime('%s', os.scraped_at) AS INTEGER)) / 3600
                AS INTEGER) AS hours_before
            FROM odds_snapshots os
            JOIN games g ON g.id = os.game_id
            WHERE os.market_group = 'Total'
              AND os.market_type = 9
              AND g.start_time IS NOT NULL
    """
    params = []
    if league_name:
        query += " AND g.league_name = ?"
        params.append(league_name)

    query += """
        )
        SELECT
            hours_before,
            COUNT(*) AS snapshots,
            COUNT(DISTINCT game_id) AS games,
            SUM(CASE WHEN line != prev_line AND prev_line IS NOT NULL THEN 1 ELSE 0 END) AS line_moves,
            ROUND(AVG(CASE WHEN line != prev_line AND prev_line IS NOT NULL
                       THEN ABS(line - prev_line) END), 1) AS avg_move_size,
            ROUND(MAX(CASE WHEN prev_line IS NOT NULL
                       THEN ABS(line - prev_line) END), 1) AS max_move_size
        FROM line_changes
        WHERE hours_before >= 0 AND hours_before <= 48
        GROUP BY hours_before
        ORDER BY hours_before ASC
    """
    return conn.execute(query, params).fetchall()


def get_clock_hour_patterns(conn, league_name=None):
    """Odds change activity by UTC clock hour."""
    query = """
        SELECT
            CAST(strftime('%H', os.scraped_at) AS INTEGER) AS hour_utc,
            COUNT(*) AS total_changes,
            COUNT(DISTINCT os.game_id) AS games,
            COUNT(DISTINCT g.league_name) AS leagues
        FROM odds_snapshots os
        JOIN games g ON g.id = os.game_id
    """
    params = []
    if league_name:
        query += " WHERE g.league_name = ?"
        params.append(league_name)

    query += """
        GROUP BY hour_utc
        ORDER BY hour_utc
    """
    return conn.execute(query, params).fetchall()


def get_league_summary(conn):
    """
    Per-league breakdown: game count, snapshot count, result coverage, last scrape.
    Useful for spotting gaps and planning per-league model training.
    """
    return conn.execute("""
        SELECT
            g.league_name,
            COUNT(DISTINCT g.id)                            AS total_games,
            COUNT(DISTINCT gr.id)                           AS games_with_results,
            COUNT(os.id)                                    AS total_snapshots,
            ROUND(
                100.0 * COUNT(DISTINCT gr.id) / COUNT(DISTINCT g.id), 1
            )                                               AS result_coverage_pct,
            MAX(os.scraped_at)                              AS last_scraped
        FROM games g
        LEFT JOIN odds_snapshots os ON os.game_id = g.id
        LEFT JOIN game_results gr   ON gr.game_id = g.id
        GROUP BY g.league_name
        ORDER BY total_games DESC
    """).fetchall()


# ============================================================
# INIT ON IMPORT
# ============================================================

if __name__ == "__main__":
    init_db()
    with get_db() as conn:
        stats = get_db_stats(conn)
        print(f"[DB] Stats: {stats}")
