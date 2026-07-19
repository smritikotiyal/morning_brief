# ☕ Morning Brief

> My version of the morning newspaper ritual — automated.

Growing up, everyone I respected had a newspaper habit. My father every morning,
my mum in the evenings, my nani in the afternoons. Somewhere along the way I lost
that habit. So I built this instead.

Every morning at 8am PST, Morning Brief scrapes the top 10 news headlines,
formats them into a clean email digest, and delivers them to my inbox.
No apps to open. No subscriptions to forget. Just the news, waiting for me.

---

## How It Works
Every morning at 8am PST:
Prefect Cloud triggers the flow (no laptop needed)
↓
Scrape top 10 news headlines via DuckDuckGo
↓
Format into clean HTML email digest
↓
Deliver to Gmail inbox
↓
☕ Read with morning coffee

## Stack

| Tool | Purpose |
|---|---|
| **Prefect 3** | Flow orchestration and scheduling |
| **DuckDuckGo Search API** | Free news scraping |
| **Gmail SMTP** | HTML email delivery |
| **Prefect Blocks** | Secrets management |
| **GitHub Actions** | CI/CD — auto-redeploys on every push to main |

**Cost: $0**

---

## Architecture
You push code to GitHub
↓
GitHub Actions triggers automatically
↓
Authenticates to Prefect Cloud
↓
Redeploys updated flow
↓
Prefect Cloud runs it every morning at 8am PST
on their managed infrastructure — no server needed
---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/smritikotiyal/morning_brief.git
cd morning_brief
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Set up Gmail App Password
- Enable 2FA on your Google account
- Go to Google Account → Security → App Passwords
- Create a new app password → copy the 16-character key (no spaces)

### 3. Set up Prefect Cloud
```bash
# Sign up free at app.prefect.cloud
prefect cloud login

# Create managed work pool
prefect work-pool create my-managed-pool --type prefect:managed

# Create secrets as Prefect Blocks
python -c "
from prefect.blocks.system import Secret
Secret(value='your.email@gmail.com').save('gmail-address')
Secret(value='xxxxxxxxxxxxxxxx').save('gmail-app-password')
Secret(value='your.email@gmail.com').save('recipient-email')
"
```

### 4. Deploy
```bash
python morning_brief.py
```

### 5. Set up CI/CD
Add these secrets to your GitHub repo (Settings → Secrets → Actions):
PREFECT_API_KEY   → your Prefect Cloud API key
PREFECT_API_URL   → your Prefect Cloud workspace URL

Push to main — GitHub Actions handles the rest.

---

## Roadmap

- [ ] Source filtering — exclude blogs, keep only legit news outlets
- [ ] Topic customization — choose your own news categories
- [ ] Summary via LLM — one-line summary per article
- [ ] Slack delivery option alongside email
- [ ] Weekly digest mode

---

## What I Learned

Building this without AI or LLMs was intentional — and refreshing.
Sometimes the right tool is a 30-line Python script and a cron job.

---

*Built on a Saturday night, two days before turning 32.*
*Happy to be reading the news again.* ☕
