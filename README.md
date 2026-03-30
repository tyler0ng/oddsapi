# 1xBet Basketball Odds Tracker

24/7 basketball odds scraper that collects line movements from 1xBet, stores them in SQLite, fetches game results from API-Basketball, and enables closing-line-vs-actual-result EV analysis.

## Status

- **Scraper deployed** on Oracle Cloud (140.245.98.229) — running 24/7 via systemd
- **Telegram alerts** — configured, sends error alerts when scraping fails
- **Results fetcher** — cron job runs every 6h

## Commands

### Scraper

```bash
python scheduler.py              # run the main loop (24/7)
python scheduler.py --once       # single scrape cycle then exit
python scheduler.py --status     # show DB stats and recent runs
```

### Results

```bash
python results_fetcher.py        # fetch scores for games missing results
python results_fetcher.py --test # test API-Basketball connection
```

### Notifications

```bash
python notifier.py               # send a test Telegram message
```

### Analysis Queries

```bash
# Browse data
python query.py leagues                          # per-league summary: games, snapshots, result coverage
python query.py games                            # list all tracked games
python query.py games "South Korea KBL"          # games for a specific league
python query.py odds 702373662                   # latest odds for a game
python query.py history 702373662 Total          # line movement history
python query.py coverage                         # per-game scrape iterations + snapshot counts
python query.py coverage "South Korea KBL"       # same, filtered to one league
python query.py stats                            # database health check
python query.py results                          # games with results

# Line movements
python query.py movements                        # find biggest line moves

# Accuracy: closing line vs actual result
python query.py accuracy                         # totals (game total)
python query.py accuracy "South Korea KBL"       # totals for one league
python query.py accuracy-team-totals             # team total line vs actual team score
python query.py accuracy-team-totals "South Korea KBL"  # team totals for one league
python query.py accuracy-team-totals --sort=delta       # sorted by biggest delta
python query.py accuracy-spread                  # handicap
python query.py accuracy-spread "NBA"            # handicap for one league

# Advanced analysis
python query.py clv                              # closing line value: opening vs closing vs result
python query.py clv "NBA"                        # CLV for one league
python query.py bias                             # over/under bias by league (historical)
python query.py time-patterns                    # line movement by hours-before-tipoff + clock hour
python query.py time-patterns "NBA"              # time patterns for one league
```

## Architecture

**Data flow:** `scheduler.py` → `scraper.py` → `database.py` → `query.py` (analysis)

- `scheduler.py` — Main loop. Calls `run_cycle()` every 300s: discovers leagues, iterates games, stores odds. Sends Telegram alerts on errors via `notifier.py`.
- `scraper.py` — HTTP layer. Uses `curl_cffi` with `impersonate="safari17_0"` to bypass TLS fingerprinting.
- `database.py` — SQLite with WAL mode. Tables: `games`, `odds_snapshots`, `scraper_runs`, `scraper_run_leagues`, `game_results`. Deduplicates snapshots (only stores when odds change).
- `results_fetcher.py` — Pulls final scores from API-Basketball (api-sports.io). Matches games via fuzzy team-name similarity.
- `query.py` — Read-only CLI for inspecting data and running analysis.
- `notifier.py` — Telegram alerts via `urllib.request`. No-op when `TELEGRAM_BOT_TOKEN` is empty.
- `config.py` — All settings. Secrets loaded from `.env` via `python-dotenv`.

## Environment Variables

All secrets live in `.env` (loaded by `python-dotenv`):

- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — Telegram alerts (empty = disabled)
- `XBET_BASE_URL` — 1xBet mirror (default: `https://1xbet.tz`)
- `API_BASKETBALL_KEY` — api-sports.io key
- `DB_PATH` — SQLite file path (default: `odds_tracker.db`)
- `XBET_SESSION`, `XBET_PAY_SESSION`, `XBET_CHE_G`, `XBET_AUID` — 1xBet cookies (refresh when scraping fails)

## Server Management

```bash
# SSH in
ssh -i "C:\Users\tyler\Downloads\ssh-key-2026-03-28.key" opc@140.245.98.229

# Check status
sudo systemctl status odds-tracker
sudo journalctl -u odds-tracker -f          # live logs

# Restart after code changes
cd ~/odds-tracker && git pull
sudo systemctl restart odds-tracker

# Run queries
cd ~/odds-tracker && source venv/bin/activate
python query.py leagues

# DB stats
python scheduler.py --status

# Check results cron log
cat /home/opc/odds-tracker/results.log
```

### Cookie Refresh

When you see 401/403 errors in the logs:
1. Open 1xBet in a browser, grab fresh cookies from DevTools
2. Update `XBET_*` vars in server `.env`
3. `sudo systemctl restart odds-tracker`

### Copy DB Locally

```bash
scp -i "C:\Users\tyler\Downloads\ssh-key-2026-03-28.key" opc@140.245.98.229:/home/opc/odds-tracker/odds_tracker.db .
```
