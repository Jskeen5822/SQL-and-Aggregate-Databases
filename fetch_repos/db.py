from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Dict, Any
import json
from datetime import datetime, timezone


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._configure()
        # Ensure schema exists so ad-hoc usage (e.g., one-liners) works without manual calls
        self.init_schema()

    def _configure(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS repositories (
                repo_id INTEGER PRIMARY KEY,
                name TEXT,
                full_name TEXT,
                owner_login TEXT,
                private INTEGER,
                fork INTEGER,
                html_url TEXT,
                description TEXT,
                created_at TEXT,
                updated_at TEXT,
                pushed_at TEXT,
                stargazers_count INTEGER,
                watchers_count INTEGER,
                forks_count INTEGER,
                open_issues_count INTEGER,
                language TEXT,
                size INTEGER,
                license TEXT,
                archived INTEGER,
                disabled INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_repositories_language ON repositories(language);
            CREATE INDEX IF NOT EXISTS idx_repositories_stars ON repositories(stargazers_count DESC);

            CREATE TABLE IF NOT EXISTS languages (
                repo_id INTEGER,
                language TEXT,
                bytes INTEGER,
                PRIMARY KEY (repo_id, language),
                FOREIGN KEY (repo_id) REFERENCES repositories(repo_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS contributors (
                repo_id INTEGER,
                login TEXT,
                contributions INTEGER,
                PRIMARY KEY (repo_id, login),
                FOREIGN KEY (repo_id) REFERENCES repositories(repo_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS aggregates (
                metric TEXT,
                key TEXT,
                value REAL,
                computed_at TEXT,
                extra_json TEXT,
                PRIMARY KEY (metric, key)
            );
            """
        )
        self.conn.commit()

    def upsert_repositories(self, repos: Iterable[Dict[str, Any]]) -> None:
        rows = []
        for r in repos:
            rows.append(
                (
                    r.get("id"),
                    r.get("name"),
                    r.get("full_name"),
                    r.get("owner", {}).get("login"),
                    1 if r.get("private") else 0,
                    1 if r.get("fork") else 0,
                    r.get("html_url"),
                    r.get("description"),
                    r.get("created_at"),
                    r.get("updated_at"),
                    r.get("pushed_at"),
                    r.get("stargazers_count"),
                    r.get("watchers_count"),
                    r.get("forks_count"),
                    r.get("open_issues_count"),
                    r.get("language"),
                    r.get("size"),
                    (r.get("license") or {}).get("name") if r.get("license") else None,
                    1 if r.get("archived") else 0,
                    1 if r.get("disabled") else 0,
                )
            )
        with self.transaction():
            self.conn.executemany(
                """
                INSERT INTO repositories (
                    repo_id,name,full_name,owner_login,private,fork,html_url,description,
                    created_at,updated_at,pushed_at,stargazers_count,watchers_count,forks_count,
                    open_issues_count,language,size,license,archived,disabled
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(repo_id) DO UPDATE SET
                    name=excluded.name,
                    full_name=excluded.full_name,
                    owner_login=excluded.owner_login,
                    private=excluded.private,
                    fork=excluded.fork,
                    html_url=excluded.html_url,
                    description=excluded.description,
                    created_at=excluded.created_at,
                    updated_at=excluded.updated_at,
                    pushed_at=excluded.pushed_at,
                    stargazers_count=excluded.stargazers_count,
                    watchers_count=excluded.watchers_count,
                    forks_count=excluded.forks_count,
                    open_issues_count=excluded.open_issues_count,
                    language=excluded.language,
                    size=excluded.size,
                    license=excluded.license,
                    archived=excluded.archived,
                    disabled=excluded.disabled
                ;
                """,
                rows,
            )

    def upsert_languages(self, items: Iterable[Dict[str, Any]]) -> None:
        rows = []
        for it in items:
            repo_id = it["repo_id"]
            for lang, b in it["languages"].items():
                rows.append((repo_id, lang, int(b)))
        with self.transaction():
            self.conn.executemany(
                """
                INSERT INTO languages(repo_id, language, bytes) VALUES (?,?,?)
                ON CONFLICT(repo_id, language) DO UPDATE SET bytes=excluded.bytes;
                """,
                rows,
            )

    def upsert_contributors(self, items: Iterable[Dict[str, Any]]) -> None:
        rows = []
        for it in items:
            repo_id = it["repo_id"]
            for c in it["contributors"]:
                rows.append((repo_id, c.get("login"), int(c.get("contributions", 0))))
        with self.transaction():
            self.conn.executemany(
                """
                INSERT INTO contributors(repo_id, login, contributions) VALUES (?,?,?)
                ON CONFLICT(repo_id, login) DO UPDATE SET contributions=excluded.contributions;
                """,
                rows,
            )

    def upsert_aggregate(self, metric: str, key: str | None, value: float, extra: Any | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        extra_json = json.dumps(extra) if extra is not None else None
        with self.transaction():
            self.conn.execute(
                """
                INSERT INTO aggregates(metric, key, value, computed_at, extra_json)
                VALUES (?,?,?,?,?)
                ON CONFLICT(metric, key) DO UPDATE SET
                    value=excluded.value,
                    computed_at=excluded.computed_at,
                    extra_json=excluded.extra_json
                ;
                """,
                (metric, key, float(value), now, extra_json),
            )
