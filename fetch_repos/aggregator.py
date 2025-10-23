from __future__ import annotations
from typing import Any
from .db import Database


""" This computes and stores summary metrics from the repositories table and into
    an aggregate stored in the aggregates table.
"""


def compute_aggregates(db: Database) -> None:
    
    cur = db.conn.cursor()

    # total_repos
    cur.execute(f"SELECT COUNT(*) FROM {db.repositories_table};")
    total_repos = cur.fetchone()[0]
    db.upsert_aggregate("total_repos", None, float(total_repos))

    # total_stars
    cur.execute(f"SELECT COALESCE(SUM(stargazers_count),0) FROM {db.repositories_table};")
    total_stars = cur.fetchone()[0] or 0
    db.upsert_aggregate("total_stars", None, float(total_stars))

    # stars_by_language
    cur.execute(
        f"""
        SELECT language, COALESCE(SUM(stargazers_count),0) AS stars
        FROM {db.repositories_table}
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY stars DESC;
        """
    )
    for lang, stars in cur.fetchall():
        db.upsert_aggregate("stars_by_language", lang, float(stars))

    # forks_by_language
    cur.execute(
        f"""
        SELECT language, COALESCE(SUM(forks_count),0) AS forks
        FROM {db.repositories_table}
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY forks DESC;
        """
    )
    for lang, forks in cur.fetchall():
        db.upsert_aggregate("forks_by_language", lang, float(forks))

    # top_repos_by_stars (top 10)
    cur.execute(
        f"""
        SELECT full_name, stargazers_count
        FROM {db.repositories_table}
        ORDER BY stargazers_count DESC, full_name ASC
        LIMIT 10;
        """
    )
    top = [{"full_name": r[0], "stars": int(r[1])} for r in cur.fetchall()]
    db.upsert_aggregate("top_repos_by_stars", None, float(len(top)), extra = top)

    # Optional DORA aggregates if DORA tables exist
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    if db.commit_activity_table in tables:
        # Sum commits per week across all repos in this namespace (limit to last 26 weeks for brevity)
        cur.execute(
            f"""
            SELECT week_start, SUM(total) as commits
            FROM {db.commit_activity_table}
            GROUP BY week_start
            ORDER BY week_start DESC
            LIMIT 26;
            """
        )
        rows = cur.fetchall()
        series = [
            {"week_start": r[0], "commits": int(r[1])}
            for r in rows
        ][::-1]  # oldest first
        db.upsert_aggregate("dora_commits_by_week", None, float(len(series)), extra=series)

    if db.pull_requests_table in tables:
        # PR counts by state (total)
        cur.execute(
            f"""
            SELECT state, COUNT(*)
            FROM {db.pull_requests_table}
            GROUP BY state;
            """
        )
        totals = {r[0] or "unknown": int(r[1]) for r in cur.fetchall()}
        total_all = sum(totals.values())
        db.upsert_aggregate("dora_prs_total", None, float(total_all), extra=totals)
