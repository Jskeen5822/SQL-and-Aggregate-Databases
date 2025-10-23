import os
import sys
import csv
import sqlite3

DB_DEFAULT = "data.db"


def export_top_repos(con: sqlite3.Connection, path: str, table: str, limit: int = 50) -> None:
    rows = con.execute(
        f"""
        SELECT full_name, stargazers_count AS stars, forks_count AS forks, language
        FROM {table}
        ORDER BY stargazers_count DESC, full_name ASC
        LIMIT ?;
        """,
        (limit,),
    ).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["full_name", "stars", "forks", "language"])
        w.writerows(rows)


def export_stars_by_language(con: sqlite3.Connection, path: str, table: str) -> None:
    rows = con.execute(
        f"""
        SELECT language, SUM(stargazers_count) AS total_stars
        FROM {table}
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY total_stars DESC, language ASC;
        """
    ).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["language", "total_stars"])
        w.writerows(rows)


def _sanitize(ns: str | None) -> str | None:
    if not ns:
        return None
    import re
    cleaned = re.sub(r"[^0-9A-Za-z_]+", "_", ns).strip("_").lower()
    return cleaned[:40] if cleaned else None


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.getenv("DB_PATH", DB_DEFAULT)
    out_dir = sys.argv[2] if len(sys.argv) > 2 else os.getcwd()
    namespace = sys.argv[3] if len(sys.argv) > 3 else os.getenv("DB_NAMESPACE")
    namespace = _sanitize(namespace)

    con = sqlite3.connect(db_path)

    # Determine repositories table based on namespace
    table = f"repositories_{namespace}" if namespace else "repositories"
    # If specified table doesn't exist but a single namespaced table exists, pick it
    existing = [name for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    if table not in existing:
        repos_like = [t for t in existing if t.startswith("repositories_")]
        if not namespace and len(repos_like) == 1:
            table = repos_like[0]

    os.makedirs(out_dir, exist_ok=True)
    export_top_repos(con, os.path.join(out_dir, "top_repos.csv"), table)
    export_stars_by_language(con, os.path.join(out_dir, "stars_by_language.csv"), table)

    # Optional DORA exports if tables exist
    commit_table = f"commit_activity_{namespace}" if namespace else "commit_activity"
    pr_table = f"pull_requests_{namespace}" if namespace else "pull_requests"
    if commit_table in existing:
        # export commits by week aggregated across repos (with repo names)
        sql = f"""
            SELECT r.full_name, c.week_start, c.total
            FROM {table} r
            JOIN {commit_table} c ON r.repo_id=c.repo_id
            ORDER BY c.week_start ASC, r.full_name ASC;
        """
        rows = con.execute(sql).fetchall()
        with open(os.path.join(out_dir, "commit_activity.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["full_name", "week_start", "commits"])
            w.writerows(rows)
    if pr_table in existing:
        sql = f"""
            SELECT r.full_name, p.number, p.state, p.created_at, p.merged_at, p.closed_at
            FROM {table} r
            JOIN {pr_table} p ON r.repo_id=p.repo_id
            ORDER BY p.created_at DESC;
        """
        rows = con.execute(sql).fetchall()
        with open(os.path.join(out_dir, "pull_requests.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["full_name", "number", "state", "created_at", "merged_at", "closed_at"])
            w.writerows(rows)

    print(f"Wrote: {os.path.join(out_dir, 'top_repos.csv')} and {os.path.join(out_dir, 'stars_by_language.csv')}")


if __name__ == "__main__":
    main()
