"""
One-time cleanup: merge duplicate game rows where 1xBet reissued the game ID
for the same matchup (same league, teams, start_time).

For each duplicate group:
  1. Pick the canonical row = the one with the most snapshots.
     (Tie-breaker: row with latest last-scraped snapshot.)
  2. Reassign all snapshots from duplicates to the canonical row.
  3. Delete game_results rows for the duplicates (the canonical keeps its own
     result; if it has none, any duplicate's result is promoted).
  4. Delete the duplicate game rows.

Usage:
  python cleanup_duplicates.py            # dry run (prints what would change)
  python cleanup_duplicates.py --apply    # actually modify the DB
"""

import sys
from database import get_db


def find_duplicate_groups(conn):
    """Return list of (league_id, home_team, away_team, start_time, count)."""
    return conn.execute("""
        SELECT league_id, home_team, away_team, start_time, COUNT(*) AS n
        FROM games
        WHERE start_time IS NOT NULL
        GROUP BY league_id, home_team, away_team, start_time
        HAVING n > 1
        ORDER BY n DESC, start_time DESC
    """).fetchall()


def rows_in_group(conn, league_id, home_team, away_team, start_time):
    """Return all game rows in one duplicate group with snapshot stats."""
    return conn.execute("""
        SELECT
            g.id,
            g.ext_game_id,
            g.league_name,
            g.first_seen,
            (SELECT COUNT(*) FROM odds_snapshots os WHERE os.game_id = g.id) AS snap_count,
            (SELECT MAX(scraped_at) FROM odds_snapshots os WHERE os.game_id = g.id) AS last_snap,
            (SELECT COUNT(*) FROM game_results gr WHERE gr.game_id = g.id) AS has_result,
            (SELECT home_score FROM game_results gr WHERE gr.game_id = g.id) AS home_score,
            (SELECT away_score FROM game_results gr WHERE gr.game_id = g.id) AS away_score
        FROM games g
        WHERE g.league_id = ? AND g.home_team = ? AND g.away_team = ?
          AND g.start_time = ?
        ORDER BY snap_count DESC, last_snap DESC
    """, (league_id, home_team, away_team, start_time)).fetchall()


def merge_group(conn, canonical_id, duplicate_ids, apply=False):
    """Move snapshots from duplicates to canonical, delete duplicate rows."""
    # Reassign snapshots
    placeholders = ",".join("?" * len(duplicate_ids))
    moved = conn.execute(
        f"SELECT COUNT(*) FROM odds_snapshots WHERE game_id IN ({placeholders})",
        duplicate_ids
    ).fetchone()[0]

    if apply:
        conn.execute(
            f"UPDATE odds_snapshots SET game_id = ? WHERE game_id IN ({placeholders})",
            [canonical_id] + duplicate_ids
        )
        # Also update scraper_run_leagues? No — it keys on league_name/run_id, not game_id.

        # If canonical has no result but a duplicate does, promote it first
        canon_result = conn.execute(
            "SELECT 1 FROM game_results WHERE game_id = ?", (canonical_id,)
        ).fetchone()
        if not canon_result:
            dup_result = conn.execute(
                f"SELECT game_id FROM game_results WHERE game_id IN ({placeholders}) LIMIT 1",
                duplicate_ids
            ).fetchone()
            if dup_result:
                conn.execute(
                    "UPDATE game_results SET game_id = ? WHERE game_id = ?",
                    (canonical_id, dup_result[0])
                )

        # Delete duplicate results (they'll be re-fetched by results_fetcher)
        conn.execute(
            f"DELETE FROM game_results WHERE game_id IN ({placeholders})",
            duplicate_ids
        )
        # Delete duplicate game rows
        conn.execute(
            f"DELETE FROM games WHERE id IN ({placeholders})",
            duplicate_ids
        )

    return moved


def main():
    apply = "--apply" in sys.argv

    with get_db() as conn:
        groups = find_duplicate_groups(conn)

        if not groups:
            print("No duplicate game groups found. Nothing to do.")
            return

        print(f"Found {len(groups)} duplicate group(s):\n")

        total_dupes_removed = 0
        total_snaps_moved = 0
        total_results_deleted = 0

        for grp in groups:
            rows = rows_in_group(
                conn, grp["league_id"], grp["home_team"],
                grp["away_team"], grp["start_time"]
            )
            canonical = rows[0]
            dupes = rows[1:]

            matchup = f"{grp['home_team']} vs {grp['away_team']}"
            print(f"  [{canonical['league_name']}] {matchup}")
            print(f"    start_time: {grp['start_time']}")
            for r in rows:
                tag = "KEEP " if r["id"] == canonical["id"] else "MERGE"
                result_str = ""
                if r["has_result"]:
                    result_str = f" result={r['home_score']}-{r['away_score']}"
                print(f"    {tag}  ext_id={r['ext_game_id']:>10}  "
                      f"snaps={r['snap_count']:>5}  last_snap={r['last_snap'] or 'never':<20}"
                      f"{result_str}")

            dup_ids = [r["id"] for r in dupes]
            moved = merge_group(conn, canonical["id"], dup_ids, apply=apply)

            dup_results = sum(r["has_result"] for r in dupes)
            total_dupes_removed += len(dupes)
            total_snaps_moved += moved
            total_results_deleted += dup_results
            print()

        mode = "APPLIED" if apply else "DRY RUN"
        print(f"[{mode}] Summary:")
        print(f"  Duplicate game rows removed: {total_dupes_removed}")
        print(f"  Snapshots reassigned to canonical: {total_snaps_moved}")
        print(f"  Duplicate game_results deleted: {total_results_deleted}")

        if not apply:
            print("\nRe-run with --apply to execute.")
        else:
            print("\nNext step: run `python results_fetcher.py` to re-fetch any now-missing results.")


if __name__ == "__main__":
    main()
