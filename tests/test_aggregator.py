from fetch_repos.db import Database
from fetch_repos.aggregator import compute_aggregates


def test_compute_aggregates_basic(tmp_path):
    db_path = tmp_path / "test.db"
    db = Database(str(db_path))
    db.init_schema()

    # Insert sample repositories
    repos = [
        {
            "id": 1,
            "name": "a",
            "full_name": "u/a",
            "owner": {"login": "u"},
            "private": False,
            "fork": False,
            "html_url": "",
            "description": "",
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2020-01-01T00:00:00Z",
            "pushed_at": "2020-01-01T00:00:00Z",
            "stargazers_count": 10,
            "watchers_count": 10,
            "forks_count": 2,
            "open_issues_count": 1,
            "language": "Python",
            "size": 100,
            "license": {"name": "MIT"},
            "archived": False,
            "disabled": False,
        },
        {
            "id": 2,
            "name": "b",
            "full_name": "u/b",
            "owner": {"login": "u"},
            "private": False,
            "fork": False,
            "html_url": "",
            "description": "",
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2020-01-01T00:00:00Z",
            "pushed_at": "2020-01-01T00:00:00Z",
            "stargazers_count": 5,
            "watchers_count": 5,
            "forks_count": 3,
            "open_issues_count": 0,
            "language": "JavaScript",
            "size": 200,
            "license": {"name": "Apache-2.0"},
            "archived": False,
            "disabled": False,
        },
        {
            "id": 3,
            "name": "c",
            "full_name": "u/c",
            "owner": {"login": "u"},
            "private": False,
            "fork": False,
            "html_url": "",
            "description": "",
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2020-01-01T00:00:00Z",
            "pushed_at": "2020-01-01T00:00:00Z",
            "stargazers_count": 0,
            "watchers_count": 0,
            "forks_count": 0,
            "open_issues_count": 0,
            "language": "Python",
            "size": 50,
            "license": None,
            "archived": False,
            "disabled": False,
        },
    ]
    db.upsert_repositories(repos)

    # Insert languages
    db.upsert_languages(
        [
            {"repo_id": 1, "languages": {"Python": 1000, "C": 10}},
            {"repo_id": 2, "languages": {"JavaScript": 500}},
            {"repo_id": 3, "languages": {"Python": 200}},
        ]
    )

    compute_aggregates(db)

    cur = db.conn.cursor()
    cur.execute("SELECT metric, key, value FROM aggregates;")
    rows = { (r[0], r[1]): r[2] for r in cur.fetchall() }

    assert rows[("total_repos", None)] == 3.0
    assert rows[("total_stars", None)] == 15.0
    # Python: 10 + 0 stars = 10; JavaScript: 5
    assert rows[("stars_by_language", "Python")] == 10.0
    assert rows[("stars_by_language", "JavaScript")] == 5.0

    # Forks by language: Python: 2 + 0 = 2; JavaScript: 3
    assert rows[("forks_by_language", "Python")] == 2.0
    assert rows[("forks_by_language", "JavaScript")] == 3.0

    # Top repos extra_json entry exists
    cur.execute("SELECT extra_json FROM aggregates WHERE metric='top_repos_by_stars' AND key IS NULL;")
    extra_json = cur.fetchone()[0]
    assert extra_json is not None and "u/a" in extra_json

    db.close()
