# 🐍 GitHub Repository Data Fetcher

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
