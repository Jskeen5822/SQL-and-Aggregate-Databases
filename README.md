## GitHub Repository Aggregator (Python)

Fetch data from all GitHub repositories for a user or organization, store it in a SQLite database, and compute useful aggregates in minutes.

### What it does
- Fetches repos via the GitHub REST API (with pagination)
- Stores to SQLite tables: repositories, languages, contributors, aggregates
- Computes aggregates: total repos, total stars, stars_by_language, forks_by_language, top repos by stars
- Fast by default (async + concurrency), designed to finish well under 15 minutes with a token

---

## Requirements
- Python 3.10+
- Windows PowerShell (these examples use PowerShell syntax)

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

If your default python is different, use the fully qualified path provided by your environment.

---

## Quick start

Set your target and token (recommended to avoid low rate limits). In PowerShell:

```powershell
# who to fetch: user or org
$env:GITHUB_TARGET = "octocat"       # or your org/user
$env:GITHUB_TARGET_TYPE = "user"     # or "org"

# GitHub token (classic or fine-grained) to increase rate limits
$env:GITHUB_TOKEN = "<your_token_here>"

# optional tweaks
$env:DB_PATH = "data.db"
$env:CONCURRENCY = "10"
$env:INCLUDE_CONTRIBUTORS = "0"   # set to 1 to fetch contributors
$env:MAX_REPOS = ""               # leave empty to fetch all
```

Run the fetcher:

```powershell
python -m fetch_repos.main --user $env:GITHUB_TARGET --concurrency 10
# Or for an org
# python -m fetch_repos.main --org my-org --include-contributors
```

This creates or updates `data.db` and writes aggregates to the `aggregates` table.

---

## CLI options

```text
--user USER                  GitHub username to fetch
--org ORG                    GitHub organization to fetch
--token TOKEN                GitHub token (or set GITHUB_TOKEN)
--db PATH                    SQLite database path (default: data.db)
--include-contributors       Fetch contributor counts per repo
--concurrency N              Max parallel requests (default: 10)
--max-repos N                Limit the number of repos processed
--skip-aggregates            Skip aggregate computation
```

Environment variables are also supported: `GITHUB_TARGET`, `GITHUB_TARGET_TYPE`, `GITHUB_TOKEN`, `DB_PATH`, `INCLUDE_CONTRIBUTORS`, `CONCURRENCY`, `MAX_REPOS`.

---

## Aggregates computed
- total_repos
- total_stars
- stars_by_language (key = language)
- forks_by_language (key = language)
- top_repos_by_stars (extra_json contains top 10 repo names with stars)

All aggregates are stored in the `aggregates` table with columns: `(metric, key, value, computed_at, extra_json)`.

---

## Keeping runtime under 15 minutes
- Use a GitHub token to unlock 5,000 requests/hour.
- Keep `--concurrency` around 10–20; higher can hit secondary rate limits.
- For very large orgs, use `--max_repos` to chunk runs, or disable contributors.
- Re-runs are incremental: repositories are upserted, so you can fetch in batches.

---

## Developer notes

Run tests:

```powershell
python -m pytest -q
```

Project structure:

```
fetch_repos/
  __init__.py
  aggregator.py
  config.py
  db.py
  github_client.py
tests/
  test_aggregator.py
requirements.txt
```

### Extending
- Add more aggregates in `fetch_repos/aggregator.py`
- Add columns/tables in `fetch_repos/db.py`
- Add more API calls in `fetch_repos/github_client.py`

---

## Troubleshooting
- 403 errors with `X-RateLimit-Remaining: 0`: add a token or wait until reset.
- SQLite locked: try rerunning; journal mode WAL is enabled for better concurrency.
- Windows SSL issues: ensure latest Python and certs; `aiohttp` handles TLS by default.

## 📘 Project Overview
This project is a **Python script** designed to automatically **fetch data from all GitHub repositories** of a given user or organization, **store the data in a SQL database**, and **compute useful aggregates** such as total stars, forks, watchers, and more.  
It demonstrates skills in **API integration, database management, and data aggregation**.

---

## ⚙️ Features
- Fetches repository data using the **GitHub REST API**  
- Stores data in a **SQL database** (e.g., SQLite or MySQL)  
- Computes aggregates like:  
  - Total stars ⭐  
  - Total forks 🍴  
  - Average watchers 👀  
  - Repository count 📦  
- Automatically updates existing repository records  
- Easy to extend for more analytics

---

## 🧠 Tech Stack
- **Language:** Python 3  
- **Database:** SQLite / MySQL  
- **Libraries:**  
  - `requests` – for API calls  
  - `sqlite3` or `mysql.connector` – for database interaction  
  - `pandas` (optional) – for data aggregation and reporting

---

## 🚀 Usage
1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/repo-fetcher.git
   cd repo-fetcher
