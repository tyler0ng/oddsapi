# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A 24/7 basketball odds scraper for 1xBet that collects line movements, stores them in SQLite, fetches game results from API-Basketball, and enables closing-line-vs-actual-result EV analysis.

## Commands

```bash
python scheduler.py              # run the main loop (24/7)
python scheduler.py --once       # single scrape cycle then exit
python scheduler.py --status     # show DB stats and recent runs

python results_fetcher.py        # fetch scores for games missing results
python results_fetcher.py --test # test API-Basketball connection

python query.py leagues          # per-league summary
python query.py accuracy         # closing line vs actual (totals)
python query.py accuracy-spread  # closing line vs actual (handicap)
python query.py movements        # biggest line swings
python query.py coverage         # per-game scrape depth
python query.py accuracy-team-totals
python query.py accuracy-team-totals "South Korea KBL"

python query.py clv                  # opening vs closing line vs actual result
python query.py clv "NBA"            # CLV for one league
python query.py bias                 # over/under bias by league (historical)
python query.py time-patterns        # line movement by hours-before-tipoff + clock hour
python query.py time-patterns "NBA"  # time patterns for one league

python notifier.py               # send a test Telegram message
```

## Architecture

**Data flow:** `scheduler.py` → `scraper.py` → `database.py` → `query.py` (analysis)

- `scheduler.py` — Main loop. Calls `run_cycle()` every 300s: discovers leagues, iterates games, stores odds. Sends Telegram alerts on errors via `notifier.py`.
- `scraper.py` — HTTP layer. Uses `curl_cffi` with `impersonate="safari17_0"` to bypass TLS fingerprinting. Three endpoints: leagues → games → odds.
- `database.py` — SQLite with WAL mode. Tables: `games`, `odds_snapshots`, `scraper_runs`, `scraper_run_leagues`, `game_results`. View: `line_movements`. Deduplicates snapshots (only stores when odds change).
- `results_fetcher.py` — Pulls final scores from API-Basketball (api-sports.io). Matches games via fuzzy team-name similarity (threshold 0.70) since the two sources use different names/IDs.
- `query.py` — CLI for inspecting data. The key analysis function is `get_closing_line_vs_result()` in `database.py`.
- `notifier.py` — Telegram alerts via `urllib.request`. All functions are no-ops when `TELEGRAM_BOT_TOKEN` is empty.
- `config.py` — All settings. Secrets read from env vars with hardcoded fallbacks for local dev.

## Critical Patterns

- **Inline params per endpoint.** Each `scraper.py` function builds its own params dict. Do NOT use `**spread` or shared params — this causes 406 errors with 1xBet's API.
- **Deduplication in `store_odds_snapshot`.** Only inserts a new row if the odds value changed from the last snapshot for that game/market/line. Do not bypass this.
- **`curl_cffi`, not `requests`**, for all 1xBet calls. Standard `requests` gets blocked by TLS fingerprinting. (`requests` is fine for API-Basketball.)
- **Cookies expire frequently.** The `COOKIES` dict in `config.py` must be refreshed manually from a browser session when scraping starts failing (401/403 errors).
- **Game status only advances forward:** upcoming → live → finished. The `upsert_game` function enforces this.

## Environment Variables

All optional — hardcoded fallbacks work for local dev:
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — Telegram alerts (empty = disabled)
- `XBET_BASE_URL` — 1xBet mirror (default: `https://1xbet.tz`)
- `API_BASKETBALL_KEY` — api-sports.io key
- `DB_PATH` — SQLite file path (default: `odds_tracker.db`)
