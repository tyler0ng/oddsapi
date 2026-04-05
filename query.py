"""
Query utility — inspect your collected odds data.

Usage:
  python query.py leagues                   # per-league summary: games, snapshots, result coverage
  python query.py games                     # list all tracked games
  python query.py games "South Korea KBL"  # games for a specific league
  python query.py odds 702373662            # latest odds for a game
  python query.py history 702373662 Total   # line movement history
  python query.py coverage                  # per-game scrape iterations + snapshot counts
  python query.py coverage "South Korea KBL"  # same, filtered to one league
  python query.py stats                     # database health check
  python query.py movements                 # find biggest line moves
  python query.py results                   # games with results
  python query.py accuracy                  # closing line vs actual result (totals)
  python query.py accuracy "South Korea KBL"  # totals accuracy for one league
  python query.py accuracy-team-totals      # closing team total line vs actual team score
  python query.py accuracy-team-totals "South Korea KBL"  # team totals for one league
  python query.py accuracy-team-totals --sort=stale       # rows with stalest closing lines first
  python query.py accuracy-spread           # closing line vs actual result (handicap)
  python query.py accuracy-spread "NBA"     # handicap accuracy for one league
  python query.py clv                      # closing line value: opening vs closing vs result
  python query.py clv "NBA"               # CLV for one league
  python query.py bias                     # over/under bias by league (historical)
  python query.py time-patterns            # line movement by hours-before-tipoff + clock hour
  python query.py time-patterns "NBA"      # time patterns for one league
"""

import sys
from datetime import datetime
from database import (
    get_db, get_latest_odds, get_line_history,
    get_upcoming_games, get_db_stats, get_league_summary,
    get_closing_line_vs_result, get_closing_line_handicap_vs_result,
    get_closing_team_totals_vs_result,
    get_clv_analysis, get_league_ou_bias,
    get_hours_before_tipoff_patterns, get_clock_hour_patterns,
)


def cmd_leagues():
    """Per-league summary — games tracked, snapshots stored, result coverage."""
    with get_db() as conn:
        rows = get_league_summary(conn)

    if not rows:
        print("  No data yet.")
        return

    print(f"\n  {'League':<30} {'Games':>6} {'Results':>8} {'Coverage':>9} {'Snapshots':>10} {'Last Scraped'}")
    print(f"  {'─'*30} {'─'*6} {'─'*8} {'─'*9} {'─'*10} {'─'*20}")
    for r in rows:
        coverage = f"{r['result_coverage_pct']:.0f}%" if r['result_coverage_pct'] is not None else "—"
        last = r['last_scraped'][:16] if r['last_scraped'] else "never"
        print(f"  {r['league_name']:<30} {r['total_games']:>6} {r['games_with_results']:>8} "
              f"{coverage:>9} {r['total_snapshots']:>10} {last}")

    print(f"\n  Total leagues: {len(rows)}")


def cmd_games(league=None):
    """List all tracked games."""
    with get_db() as conn:
        games = get_upcoming_games(conn, league)
    print(f"\n  {'League':<25} {'Home':<25} {'Away':<25} {'Game ID'}")
    print(f"  {'─'*25} {'─'*25} {'─'*25} {'─'*12}")
    for g in games:
        print(f"  {g['league_name']:<25} {g['home_team']:<25} {g['away_team']:<25} {g['ext_game_id']}")
    print(f"\n  Total: {len(games)} games")


def cmd_odds(ext_game_id, market_group=None):
    """Show latest odds for a game."""
    with get_db() as conn:
        rows = get_latest_odds(conn, int(ext_game_id), market_group)

    if not rows:
        print("  No odds found for this game.")
        return

    print(f"\n  {rows[0]['home_team']} vs {rows[0]['away_team']} ({rows[0]['league_name']})")
    print(f"  {'─' * 60}")

    current_group = None
    for r in rows:
        if r["market_group"] != current_group:
            current_group = r["market_group"]
            print(f"\n  [{current_group}]")

        line_str = f" ({r['line']:+.1f})" if r['line'] is not None else ""
        main = " ★" if r["is_main_line"] else ""
        print(f"    {r['market_label']}{line_str}: {r['odds_display']}{main}")


def cmd_history(ext_game_id, market_group):
    """Show line movement history for a game's market."""
    with get_db() as conn:
        rows = get_line_history(conn, int(ext_game_id), market_group)

    if not rows:
        print("  No history found.")
        return

    print(f"\n  Line movement for game {ext_game_id} — [{market_group}]")
    print(f"  {'Time':<22} {'Line':>8} {'Odds':>8} {'Main':>5}")
    print(f"  {'─'*22} {'─'*8} {'─'*8} {'─'*5}")

    for r in rows:
        line_str = f"{r['line']:+.1f}" if r['line'] is not None else "—"
        main = "★" if r["is_main_line"] else ""
        print(f"  {r['scraped_at']:<22} {line_str:>8} {r['odds_display']:>8} {main:>5}")


def cmd_movements():
    """
    Find the biggest line movements across all tracked games.
    This is where you spot sharp action.
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                g.home_team,
                g.away_team,
                g.league_name,
                g.ext_game_id,
                os.market_group,
                os.market_label,
                os.line,
                MIN(os.odds) as min_odds,
                MAX(os.odds) as max_odds,
                MAX(os.odds) - MIN(os.odds) as odds_swing,
                COUNT(*) as snapshot_count
            FROM odds_snapshots os
            JOIN games g ON g.id = os.game_id
            WHERE os.is_main_line = 1
            GROUP BY os.game_id, os.market_group, os.market_label
            HAVING snapshot_count > 1
            ORDER BY odds_swing DESC
            LIMIT 20
        """).fetchall()

    if not rows:
        print("  No movements yet — need at least 2 scrape cycles.")
        return

    print(f"\n  {'Game':<40} {'Market':<15} {'Label':<12} {'Line':>6} {'Low':>6} {'High':>6} {'Swing':>6}")
    print(f"  {'─'*40} {'─'*15} {'─'*12} {'─'*6} {'─'*6} {'─'*6} {'─'*6}")

    for r in rows:
        matchup = f"{r['home_team']} vs {r['away_team']}"[:40]
        line_str = f"{r['line']:+.1f}" if r['line'] is not None else "—"
        print(f"  {matchup:<40} {r['market_group']:<15} {r['market_label']:<12} "
              f"{line_str:>6} {r['min_odds']:>6.2f} {r['max_odds']:>6.2f} {r['odds_swing']:>6.3f}")


def cmd_coverage(league=None):
    """
    Per-game scrape history: how many times each game was captured,
    total snapshots stored, and tracking window.
    """
    with get_db() as conn:
        query = """
            SELECT
                g.league_name,
                g.home_team,
                g.away_team,
                g.ext_game_id,
                g.first_seen,
                MAX(os.scraped_at)                          AS last_scraped,
                COUNT(DISTINCT os.scraped_at)               AS scrape_iterations,
                COUNT(os.id)                                AS total_snapshots,
                COUNT(DISTINCT os.market_group)             AS markets_tracked
            FROM games g
            LEFT JOIN odds_snapshots os ON os.game_id = g.id
        """
        params = []
        if league:
            query += " WHERE g.league_name = ?"
            params.append(league)
        query += """
            GROUP BY g.id
            ORDER BY scrape_iterations DESC, g.first_seen DESC
        """
        rows = conn.execute(query, params).fetchall()

    if not rows:
        print("  No data yet.")
        return

    print(f"\n  {'League':<25} {'Game':<45} {'Iters':>6} {'Snaps':>7} {'Mkts':>5} {'First Seen':<18} {'Last Scraped'}")
    print(f"  {'─'*25} {'─'*45} {'─'*6} {'─'*7} {'─'*5} {'─'*18} {'─'*18}")
    for r in rows:
        matchup = f"{r['home_team']} vs {r['away_team']}"[:45]
        first = r['first_seen'][:16] if r['first_seen'] else "—"
        last  = r['last_scraped'][:16] if r['last_scraped'] else "—"
        print(f"  {r['league_name']:<25} {matchup:<45} {r['scrape_iterations']:>6} "
              f"{r['total_snapshots']:>7} {r['markets_tracked']:>5} {first:<18} {last}")

    total_iters = sum(r['scrape_iterations'] for r in rows)
    total_snaps = sum(r['total_snapshots'] for r in rows)
    print(f"\n  {len(rows)} games | {total_iters} total iterations | {total_snaps} total snapshots")


def cmd_stats():
    """Database health check."""
    with get_db() as conn:
        stats = get_db_stats(conn)
    print("\n  [Database Stats]")
    for k, v in stats.items():
        print(f"    {k}: {v}")


def cmd_results():
    """Show games that have results stored."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT g.league_name, g.home_team, g.away_team, g.ext_game_id,
                   gr.home_score, gr.away_score, gr.total_points, gr.status
            FROM game_results gr
            JOIN games g ON g.id = gr.game_id
            ORDER BY gr.fetched_at DESC
        """).fetchall()

    if not rows:
        print("  No results stored yet. Run: python results_fetcher.py")
        return

    print(f"\n  {'Game':<45} {'Score':>10} {'Total':>6} {'League'}")
    print(f"  {'─'*45} {'─'*10} {'─'*6} {'─'*25}")
    for r in rows:
        matchup = f"{r['home_team']} vs {r['away_team']}"[:45]
        score = f"{r['home_score']}-{r['away_score']}"
        print(f"  {matchup:<45} {score:>10} {r['total_points']:>6} {r['league_name']}")
    print(f"\n  Total: {len(rows)} results")


def cmd_accuracy(league=None):
    """THE KEY ANALYSIS: closing total line vs actual result."""
    with get_db() as conn:
        rows = get_closing_line_vs_result(conn, league)

    if not rows:
        print("  No results with closing lines yet.")
        return

    print(f"\n  CLOSING LINE vs ACTUAL RESULT (Totals)")
    print(f"  {'Game':<40} {'Line':>7} {'Actual':>7} {'Delta':>7} {'Result':>7}")
    print(f"  {'─'*40} {'─'*7} {'─'*7} {'─'*7} {'─'*7}")

    over_count = 0
    under_count = 0
    push_count = 0
    total_delta = 0

    for r in rows:
        matchup = f"{r['home_team']} vs {r['away_team']}"[:40]
        line = f"{r['closing_total_line']:.1f}" if r['closing_total_line'] else "N/A"
        delta = f"{r['delta']:+.1f}" if r['delta'] is not None else "N/A"

        print(f"  {matchup:<40} {line:>7} {r['total_points']:>7} {delta:>7} {r['result']:>7}")

        if r['result'] == 'OVER':
            over_count += 1
        elif r['result'] == 'UNDER':
            under_count += 1
        else:
            push_count += 1

        if r['delta'] is not None:
            total_delta += abs(r['delta'])

    total = over_count + under_count + push_count
    avg_delta = total_delta / total if total else 0

    print(f"\n  Summary:")
    print(f"    Games analyzed: {total}")
    print(f"    OVER:  {over_count} ({over_count/total*100:.0f}%)" if total else "")
    print(f"    UNDER: {under_count} ({under_count/total*100:.0f}%)" if total else "")
    print(f"    PUSH:  {push_count}")
    print(f"    Avg absolute delta: {avg_delta:.1f} points")


def cmd_accuracy_spread(league=None):
    """Closing handicap line vs actual margin."""
    with get_db() as conn:
        rows = get_closing_line_handicap_vs_result(conn, league)

    if not rows:
        print("  No results with closing handicap lines yet.")
        return

    print(f"\n  CLOSING LINE vs ACTUAL RESULT (Handicap)")
    print(f"  {'Game':<40} {'HC':>7} {'Margin':>7} {'Result':>13}")
    print(f"  {'─'*40} {'─'*7} {'─'*7} {'─'*13}")

    home_covers = 0
    away_covers = 0

    for r in rows:
        matchup = f"{r['home_team']} vs {r['away_team']}"[:40]
        hc = f"{r['closing_handicap']:+.1f}" if r['closing_handicap'] else "N/A"
        margin = f"{r['actual_margin']:+d}"

        print(f"  {matchup:<40} {hc:>7} {margin:>7} {r['result']:>13}")

        if r['result'] == 'HOME COVERS':
            home_covers += 1
        elif r['result'] == 'AWAY COVERS':
            away_covers += 1

    total = home_covers + away_covers
    if total:
        print(f"\n  Home covers: {home_covers}/{total} ({home_covers/total*100:.0f}%)")
        print(f"  Away covers: {away_covers}/{total} ({away_covers/total*100:.0f}%)")


def cmd_accuracy_team_totals(league=None, sort=None):
    """Closing team total line vs actual team score, with per-league averages."""
    sort_options = {
        'delta': 'ABS(delta) DESC',
        'score': 'actual_score DESC',
        'league': 'league_name, ext_game_id, side',
        'stale': 'seconds_to_tip DESC',
    }
    sort_order = sort_options.get(sort, 'league_name, ext_game_id, side')
    with get_db() as conn:
        rows = get_closing_team_totals_vs_result(conn, league, sort_order=sort_order)

    if not rows:
        print("  No results with closing team total lines yet.")
        return

    print(f"\n  CLOSING LINE vs ACTUAL RESULT (Team Totals)")
    print(f"  {'Date':<10} {'Team':<22} {'Side':<5} {'Line':>7} {'Actual':>7} {'Delta':>7} "
          f"{'Result':>7} {'Snaps':>6} {'ToTip':>6} {'League'}")
    print(f"  {'─'*10} {'─'*22} {'─'*5} {'─'*7} {'─'*7} {'─'*7} {'─'*7} {'─'*6} {'─'*6} {'─'*25}")

    # Collect per-league stats
    league_stats = {}
    over_count = 0
    under_count = 0
    push_count = 0
    total_delta = 0
    stale_count = 0  # closing snap > 1h before tip

    for r in rows:
        team = r['team'][:22]
        delta = f"{r['delta']:+.1f}" if r['delta'] is not None else "N/A"

        # Hours between closing-line snap and tipoff (computed in SQL, UTC-safe)
        hours_to_tip_str = "—"
        is_stale = False
        if r['seconds_to_tip'] is not None:
            gap_h = r['seconds_to_tip'] / 3600
            hours_to_tip_str = f"{gap_h:+.1f}h"
            if gap_h > 1.0:
                is_stale = True
                stale_count += 1

        # Format date from start_time
        date_str = "—"
        if r['start_time']:
            date_str = datetime.utcfromtimestamp(r['start_time']).strftime("%m-%d %H:%M")

        stale_mark = "!" if is_stale else " "
        print(f"  {date_str:<10} {team:<22} {r['side']:<5} {r['closing_line']:>7.1f} "
              f"{r['actual_score']:>7} {delta:>7} {r['result']:>7} {r['snapshots']:>6} "
              f"{hours_to_tip_str:>6}{stale_mark}{r['league_name']}")

        if r['result'] == 'OVER':
            over_count += 1
        elif r['result'] == 'UNDER':
            under_count += 1
        else:
            push_count += 1

        if r['delta'] is not None:
            total_delta += abs(r['delta'])

        # Per-league accumulation
        lg = r['league_name']
        if lg not in league_stats:
            league_stats[lg] = {'over': 0, 'under': 0, 'push': 0, 'total_score': 0, 'total_line': 0.0, 'count': 0, 'total_delta': 0.0}
        s = league_stats[lg]
        s['count'] += 1
        s['total_score'] += r['actual_score']
        s['total_line'] += r['closing_line']
        s['total_delta'] += abs(r['delta']) if r['delta'] is not None else 0
        if r['result'] == 'OVER':
            s['over'] += 1
        elif r['result'] == 'UNDER':
            s['under'] += 1
        else:
            s['push'] += 1

    total = over_count + under_count + push_count
    avg_delta = total_delta / total if total else 0

    print(f"\n  Overall Summary:")
    print(f"    Team totals analyzed: {total}")
    if total:
        print(f"    OVER:  {over_count} ({over_count/total*100:.0f}%)")
        print(f"    UNDER: {under_count} ({under_count/total*100:.0f}%)")
        print(f"    PUSH:  {push_count}")
        print(f"    Avg absolute delta: {avg_delta:.1f} points")
        if stale_count:
            print(f"    Stale closing lines (snap >1h before tip): {stale_count}/{total}  "
                  f"(marked with '!' — not a true closing line)")

    # Per-league breakdown
    if len(league_stats) > 1 or not league:
        print(f"\n  Per-League Breakdown:")
        print(f"  {'League':<30} {'N':>4} {'Avg Score':>10} {'Avg Line':>9} {'Over%':>6} {'Under%':>7} {'Avg |Δ|':>8}")
        print(f"  {'─'*30} {'─'*4} {'─'*10} {'─'*9} {'─'*6} {'─'*7} {'─'*8}")
        for lg in sorted(league_stats, key=lambda x: league_stats[x]['count'], reverse=True):
            s = league_stats[lg]
            n = s['count']
            avg_score = s['total_score'] / n
            avg_line = s['total_line'] / n
            decided = s['over'] + s['under']
            over_pct = f"{s['over']/decided*100:.0f}%" if decided else "—"
            under_pct = f"{s['under']/decided*100:.0f}%" if decided else "—"
            avg_d = s['total_delta'] / n
            print(f"  {lg:<30} {n:>4} {avg_score:>10.1f} {avg_line:>9.1f} {over_pct:>6} {under_pct:>7} {avg_d:>8.1f}")


def cmd_clv(league=None):
    """Closing Line Value: opening vs closing line vs actual result."""
    with get_db() as conn:
        rows = get_clv_analysis(conn, league)

    if not rows:
        print("  No games with both opening and closing lines + results.")
        return

    print(f"\n  CLOSING LINE VALUE (Totals)")
    print(f"  {'Game':<35} {'Open':>6} {'Close':>6} {'Move':>6} {'Actual':>7} {'vs Open':>8} {'vs Close':>9}")
    print(f"  {'─'*35} {'─'*6} {'─'*6} {'─'*6} {'─'*7} {'─'*8} {'─'*9}")

    moved_up = 0
    moved_down = 0
    no_move = 0
    total_abs_move = 0

    for r in rows:
        matchup = f"{r['home_team']} vs {r['away_team']}"[:35]
        move = f"{r['line_move']:+.1f}" if r['line_move'] else "0.0"

        print(f"  {matchup:<35} {r['opening_line']:>6.1f} {r['closing_line']:>6.1f} "
              f"{move:>6} {r['total_points']:>7} {r['result_vs_open']:>8} {r['result_vs_close']:>9}")

        if r['line_move'] and r['line_move'] > 0:
            moved_up += 1
            total_abs_move += abs(r['line_move'])
        elif r['line_move'] and r['line_move'] < 0:
            moved_down += 1
            total_abs_move += abs(r['line_move'])
        else:
            no_move += 1

    total = len(rows)
    moved = moved_up + moved_down

    # Compare: how accurate was opening line vs closing line?
    open_over = sum(1 for r in rows if r['result_vs_open'] == 'OVER')
    open_under = sum(1 for r in rows if r['result_vs_open'] == 'UNDER')
    close_over = sum(1 for r in rows if r['result_vs_close'] == 'OVER')
    close_under = sum(1 for r in rows if r['result_vs_close'] == 'UNDER')

    avg_abs_open = sum(abs(r['delta_vs_open']) for r in rows if r['delta_vs_open'] is not None) / total if total else 0
    avg_abs_close = sum(abs(r['delta_vs_close']) for r in rows if r['delta_vs_close'] is not None) / total if total else 0

    print(f"\n  Summary ({total} games):")
    print(f"    Line moved:       {moved}/{total} games")
    if moved:
        print(f"    Avg move size:    {total_abs_move / moved:.1f} pts")
        print(f"    Moved UP:         {moved_up} ({moved_up/moved*100:.0f}%)")
        print(f"    Moved DOWN:       {moved_down} ({moved_down/moved*100:.0f}%)")

    print(f"\n    Opening line accuracy:  avg |delta| = {avg_abs_open:.1f} pts")
    decided_open = open_over + open_under
    if decided_open:
        print(f"      Over: {open_over}/{decided_open} ({open_over/decided_open*100:.0f}%)  "
              f"Under: {open_under}/{decided_open} ({open_under/decided_open*100:.0f}%)")

    print(f"    Closing line accuracy:  avg |delta| = {avg_abs_close:.1f} pts")
    decided_close = close_over + close_under
    if decided_close:
        print(f"      Over: {close_over}/{decided_close} ({close_over/decided_close*100:.0f}%)  "
              f"Under: {close_under}/{decided_close} ({close_under/decided_close*100:.0f}%)")

    if avg_abs_open and avg_abs_close:
        if avg_abs_close < avg_abs_open:
            print(f"\n    Closing line is {avg_abs_open - avg_abs_close:.1f} pts more accurate (market sharpens).")
        else:
            print(f"\n    Opening line is {avg_abs_close - avg_abs_open:.1f} pts more accurate (early edge exists!).")


def cmd_bias():
    """Over/under bias by league — which leagues consistently go over or under?"""
    with get_db() as conn:
        rows = get_league_ou_bias(conn)

    if not rows:
        print("  Not enough data yet (need 3+ finished games per league).")
        return

    print(f"\n  OVER/UNDER BIAS BY LEAGUE")
    print(f"  {'League':<30} {'N':>4} {'Over':>5} {'Under':>5} {'Over%':>6} {'Avg Δ':>7} "
          f"{'Avg |Δ|':>7} {'Avg Line':>9} {'Avg Actual':>10}")
    print(f"  {'─'*30} {'─'*4} {'─'*5} {'─'*5} {'─'*6} {'─'*7} {'─'*7} {'─'*9} {'─'*10}")

    for r in rows:
        decided = r['overs'] + r['unders']
        over_pct = f"{r['overs']/decided*100:.0f}%" if decided else "—"
        avg_d = f"{r['avg_delta']:+.1f}" if r['avg_delta'] is not None else "—"
        avg_abs = f"{r['avg_abs_delta']:.1f}" if r['avg_abs_delta'] is not None else "—"

        print(f"  {r['league_name']:<30} {r['total_games']:>4} {r['overs']:>5} {r['unders']:>5} "
              f"{over_pct:>6} {avg_d:>7} {avg_abs:>7} {r['avg_closing_line']:>9.1f} {r['avg_actual_total']:>10.1f}")

    # Flag biased leagues
    biased = [r for r in rows if r['avg_delta'] is not None and abs(r['avg_delta']) >= 3]
    if biased:
        print(f"\n  Leagues with significant bias (avg delta >= 3 pts):")
        for r in biased:
            direction = "OVER" if r['avg_delta'] > 0 else "UNDER"
            print(f"    {r['league_name']}: leans {direction} by {abs(r['avg_delta']):.1f} pts on average")


def cmd_time_patterns(league=None):
    """When do lines move? Activity by hours-before-tipoff and clock hour."""
    with get_db() as conn:
        hours_rows = get_hours_before_tipoff_patterns(conn, league)
        clock_rows = get_clock_hour_patterns(conn, league)

    # --- Part 1: Hours before tip-off ---
    if hours_rows:
        print(f"\n  LINE ACTIVITY BY HOURS BEFORE TIP-OFF")
        print(f"  {'Hrs Before':>10} {'Snapshots':>10} {'Games':>6} {'Line Moves':>11} {'Avg Move':>9} {'Max Move':>9}")
        print(f"  {'─'*10} {'─'*10} {'─'*6} {'─'*11} {'─'*9} {'─'*9}")

        peak_hour = None
        peak_moves = 0

        for r in hours_rows:
            avg_m = f"{r['avg_move_size']:.1f}" if r['avg_move_size'] is not None else "—"
            max_m = f"{r['max_move_size']:.1f}" if r['max_move_size'] is not None else "—"
            print(f"  {r['hours_before']:>10}h {r['snapshots']:>10} {r['games']:>6} "
                  f"{r['line_moves']:>11} {avg_m:>9} {max_m:>9}")

            if r['line_moves'] > peak_moves:
                peak_moves = r['line_moves']
                peak_hour = r['hours_before']

        if peak_hour is not None:
            print(f"\n  Peak activity: {peak_hour}h before tip-off ({peak_moves} line moves)")
    else:
        print("\n  No hours-before-tipoff data (need games with start_time set).")

    # --- Part 2: Clock hour (UTC) ---
    if clock_rows:
        print(f"\n  ODDS CHANGE ACTIVITY BY CLOCK HOUR (UTC)")
        print(f"  {'Hour':>6} {'Changes':>8} {'Games':>6} {'Leagues':>8}")
        print(f"  {'─'*6} {'─'*8} {'─'*6} {'─'*8}")

        max_changes = max(r['total_changes'] for r in clock_rows)

        for r in clock_rows:
            bar_len = int(20 * r['total_changes'] / max_changes) if max_changes else 0
            bar = '█' * bar_len
            print(f"  {r['hour_utc']:>4}:00 {r['total_changes']:>8} {r['games']:>6} {r['leagues']:>8}  {bar}")

        peak = max(clock_rows, key=lambda r: r['total_changes'])
        print(f"\n  Busiest hour: {peak['hour_utc']:02d}:00 UTC ({peak['total_changes']} changes across {peak['games']} games)")
    else:
        print("\n  No clock-hour data yet.")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    # Skip init_db() — query.py is read-only and init_db runs migrations
    # that require write locks, which fail when the scheduler is running.
    pass

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "leagues":
        cmd_leagues()

    elif cmd == "games":
        league = sys.argv[2] if len(sys.argv) > 2 else None
        cmd_games(league)

    elif cmd == "odds":
        if len(sys.argv) < 3:
            print("Usage: python query.py odds <game_id> [market_group]")
            sys.exit(1)
        market = sys.argv[3] if len(sys.argv) > 3 else None
        cmd_odds(sys.argv[2], market)

    elif cmd == "history":
        if len(sys.argv) < 4:
            print("Usage: python query.py history <game_id> <market_group>")
            sys.exit(1)
        cmd_history(sys.argv[2], sys.argv[3])

    elif cmd == "coverage":
        league = sys.argv[2] if len(sys.argv) > 2 else None
        cmd_coverage(league)

    elif cmd == "movements":
        cmd_movements()

    elif cmd == "stats":
        cmd_stats()

    elif cmd == "results":
        cmd_results()

    elif cmd == "accuracy":
        league = sys.argv[2] if len(sys.argv) > 2 else None
        cmd_accuracy(league)

    elif cmd == "accuracy-team-totals":
        league = None
        sort = None
        for arg in sys.argv[2:]:
            if arg.startswith("--sort="):
                sort = arg.split("=", 1)[1]
            elif league is None:
                league = arg
        cmd_accuracy_team_totals(league, sort=sort)

    elif cmd == "accuracy-spread":
        league = sys.argv[2] if len(sys.argv) > 2 else None
        cmd_accuracy_spread(league)

    elif cmd == "clv":
        league = sys.argv[2] if len(sys.argv) > 2 else None
        cmd_clv(league)

    elif cmd == "bias":
        cmd_bias()

    elif cmd == "time-patterns":
        league = sys.argv[2] if len(sys.argv) > 2 else None
        cmd_time_patterns(league)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
