import os
import sys
import csv
import sqlite3

DB_DEFAULT = "data.db"


def export_top_repos(con: sqlite3.Connection, path: str, limit: int = 50) -> None:
    rows = con.execute(
        """
        SELECT full_name, stargazers_count AS stars, forks_count AS forks, language
        FROM repositories
        ORDER BY stargazers_count DESC, full_name ASC
        LIMIT ?;
        """,
        (limit,),
    ).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["full_name", "stars", "forks", "language"])
        w.writerows(rows)


def export_stars_by_language(con: sqlite3.Connection, path: str) -> None:
    rows = con.execute(
        """
        SELECT language, SUM(stargazers_count) AS total_stars
        FROM repositories
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY total_stars DESC, language ASC;
        """
    ).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["language", "total_stars"])
        w.writerows(rows)


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.getenv("DB_PATH", DB_DEFAULT)
    out_dir = sys.argv[2] if len(sys.argv) > 2 else os.getcwd()

    con = sqlite3.connect(db_path)

    os.makedirs(out_dir, exist_ok=True)
    export_top_repos(con, os.path.join(out_dir, "top_repos.csv"))
    export_stars_by_language(con, os.path.join(out_dir, "stars_by_language.csv"))

    print(f"Wrote: {os.path.join(out_dir, 'top_repos.csv')} and {os.path.join(out_dir, 'stars_by_language.csv')}")


if __name__ == "__main__":
    main()
