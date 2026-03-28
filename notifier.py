"""
Telegram notification module.

All functions are no-ops if TELEGRAM_BOT_TOKEN is empty,
so the bot works fine without Telegram configured.
"""

import json
import urllib.request
import urllib.error
from datetime import datetime
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID


def _is_configured():
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram(message):
    """Send a message via Telegram Bot API. Fails silently if misconfigured."""
    if not _is_configured():
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception as e:
        print(f"[TELEGRAM] Failed to send: {e}")


def notify_startup():
    """Send a startup notification."""
    send_telegram(
        f"🟢 <b>Odds Tracker started</b>\n"
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )


def notify_error(context, error):
    """Send an error alert."""
    send_telegram(
        f"🔴 <b>Error:</b> {context}\n"
        f"<code>{error}</code>"
    )


def notify_warning(message):
    """Send a warning (e.g. 0 leagues found)."""
    send_telegram(f"⚠️ <b>Warning:</b> {message}")


def notify_cycle_summary(leagues, games, odds, errors):
    """Send cycle summary — only if there were errors."""
    if errors == 0:
        return

    send_telegram(
        f"⚠️ <b>Cycle finished with {errors} error(s)</b>\n"
        f"Leagues: {leagues} | Games: {games} | New odds: {odds}"
    )


if __name__ == "__main__":
    if not _is_configured():
        print("Telegram not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.")
    else:
        print("Sending test message...")
        send_telegram("✅ <b>Test message</b> — Odds Tracker notifications are working!")
        print("Done. Check your Telegram.")
