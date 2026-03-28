# Oracle Cloud Deployment — Odds Tracker (Ubuntu)

## 1. Create the Instance

- Oracle Cloud → Compute → Create Instance
- Shape: **VM.Standard.A1.Flex** (Ampere ARM) — 1 OCPU, 6GB RAM
- Image: **Ubuntu 22.04** (Canonical)
- Boot volume: 46.6GB (default)
- Download your SSH key during creation

## 2. SSH In and Install Dependencies

```bash
ssh -i ~/oracle_key ubuntu@<PUBLIC_IP>

# System packages
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3.11 python3.11-venv python3-pip git

# Clone the repo
git clone <YOUR_REPO_URL> ~/odds-tracker
cd ~/odds-tracker

# Virtual environment
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 3. Configure Environment

```bash
cat > ~/odds-tracker/.env << 'EOF'
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
XBET_BASE_URL=https://1xbet.tz
API_BASKETBALL_KEY=your_key_here
DB_PATH=/home/ubuntu/odds-tracker/odds_tracker.db
EOF
```

## 4. Telegram Bot Setup

1. Message **@BotFather** on Telegram → `/newbot` → follow prompts
2. Copy the bot token into `.env`
3. To get your chat ID:
   - Message your new bot (send anything)
   - Visit `https://api.telegram.org/bot<TOKEN>/getUpdates`
   - Find `"chat":{"id":XXXXXXX}` — that's your chat ID
4. Test: `python notifier.py`

## 5. Create systemd Service

```bash
sudo tee /etc/systemd/system/odds-tracker.service << 'EOF'
[Unit]
Description=1xBet Basketball Odds Tracker
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/odds-tracker
EnvironmentFile=/home/ubuntu/odds-tracker/.env
ExecStart=/home/ubuntu/odds-tracker/venv/bin/python scheduler.py
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable odds-tracker
sudo systemctl start odds-tracker
```

## 6. Useful Commands

```bash
# Check status
sudo systemctl status odds-tracker

# View live logs
sudo journalctl -u odds-tracker -f

# Restart after code changes
cd ~/odds-tracker && git pull
sudo systemctl restart odds-tracker

# Run a single test cycle
source venv/bin/activate
python scheduler.py --once

# Check DB stats
python scheduler.py --status
```

## 7. Oracle Cloud Firewall (Optional)

No inbound ports needed — the tracker only makes outbound HTTP requests.
The default security list is fine as-is.

## 8. Keeping Sessions Fresh

1xBet cookies expire. When scraping starts failing:
1. Grab fresh cookies from a browser session
2. Update `config.py` → `COOKIES` dict
3. `git commit && git push` → SSH in → `git pull` → `sudo systemctl restart odds-tracker`

Or set up a cron to remind you every few days via Telegram.
