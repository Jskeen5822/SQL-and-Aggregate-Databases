from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Dict, Any
import json
from datetime import datetime, timezone


def _sanitize_namespace(ns: str | None) -> str | None:
    if not ns:
        return None
    # allow only alphanumerics and underscore, lower-cased, max 40 chars
    import re
    cleaned = re.sub(r"[^0-9A-Za-z_]+", "_", ns).strip("_").lower()
    return cleaned[:40] if cleaned else None


class Database:
    def __init__(self, path: str, namespace: str | None = None) -> None:
        self.path = path
        self.namespace = _sanitize_namespace(namespace)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._configure()
        # Ensure schema exists so ad-hoc usage (e.g., one-liners) works without manual calls
        self.init_schema()

    @property
    def repositories_table(self) -> str:
        return f"repositories_{self.namespace}" if self.namespace else "repositories"

    @property
    def languages_table(self) -> str:
        return f"languages_{self.namespace}" if self.namespace else "languages"

    @property
    def contributors_table(self) -> str:
        return f"contributors_{self.namespace}" if self.namespace else "contributors"

    @property
    def aggregates_table(self) -> str:
        return f"aggregates_{self.namespace}" if self.namespace else "aggregates"

    @property
    def commit_activity_table(self) -> str:
        return f"commit_activity_{self.namespace}" if self.namespace else "commit_activity"

    @property
    def pull_requests_table(self) -> str:
        return f"pull_requests_{self.namespace}" if self.namespace else "pull_requests"

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
            f"""
            CREATE TABLE IF NOT EXISTS {self.repositories_table} (
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

            CREATE INDEX IF NOT EXISTS idx_{self.repositories_table}_language ON {self.repositories_table}(language);
            CREATE INDEX IF NOT EXISTS idx_{self.repositories_table}_stars ON {self.repositories_table}(stargazers_count DESC);

            CREATE TABLE IF NOT EXISTS {self.languages_table} (
                repo_id INTEGER,
                language TEXT,
                bytes INTEGER,
                PRIMARY KEY (repo_id, language),
                FOREIGN KEY (repo_id) REFERENCES {self.repositories_table}(repo_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS {self.contributors_table} (
                repo_id INTEGER,
                login TEXT,
                contributions INTEGER,
                PRIMARY KEY (repo_id, login),
                FOREIGN KEY (repo_id) REFERENCES {self.repositories_table}(repo_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS {self.aggregates_table} (
                metric TEXT,
                key TEXT,
                value REAL,
                computed_at TEXT,
                extra_json TEXT,
                PRIMARY KEY (metric, key)
            );

            -- DORA tables
            CREATE TABLE IF NOT EXISTS {self.commit_activity_table} (
                repo_id INTEGER,
                week_start TEXT,
                total INTEGER,
                PRIMARY KEY (repo_id, week_start),
                FOREIGN KEY (repo_id) REFERENCES {self.repositories_table}(repo_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS {self.pull_requests_table} (
                repo_id INTEGER,
                number INTEGER,
                state TEXT,
                created_at TEXT,
                merged_at TEXT,
                closed_at TEXT,
                PRIMARY KEY (repo_id, number),
                FOREIGN KEY (repo_id) REFERENCES {self.repositories_table}(repo_id) ON DELETE CASCADE
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
                f"""
                INSERT INTO {self.repositories_table} (
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
                f"""
                INSERT INTO {self.languages_table}(repo_id, language, bytes) VALUES (?,?,?)
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
                f"""
                INSERT INTO {self.contributors_table}(repo_id, login, contributions) VALUES (?,?,?)
                ON CONFLICT(repo_id, login) DO UPDATE SET contributions=excluded.contributions;
                """,
                rows,
            )

    def upsert_aggregate(self, metric: str, key: str | None, value: float, extra: Any | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        extra_json = json.dumps(extra) if extra is not None else None
        with self.transaction():
            self.conn.execute(
                f"""
                INSERT INTO {self.aggregates_table}(metric, key, value, computed_at, extra_json)
                VALUES (?,?,?,?,?)
                ON CONFLICT(metric, key) DO UPDATE SET
                    value=excluded.value,
                    computed_at=excluded.computed_at,
                    extra_json=excluded.extra_json
                ;
                """,
                (metric, key, float(value), now, extra_json),
            )

    def upsert_commit_activity(self, items: Iterable[Dict[str, Any]]) -> None:
        rows = []
        for it in items:
            rows.append((it["repo_id"], it["week_start"], int(it["total"])) )
        if not rows:
            return
        with self.transaction():
            self.conn.executemany(
                f"""
                INSERT INTO {self.commit_activity_table}(repo_id, week_start, total) VALUES (?,?,?)
                ON CONFLICT(repo_id, week_start) DO UPDATE SET total=excluded.total;
                """,
                rows,
            )

    def upsert_pull_requests(self, items: Iterable[Dict[str, Any]]) -> None:
        rows = []
        for it in items:
            rows.append((
                it["repo_id"], int(it["number"]), it.get("state"),
                it.get("created_at"), it.get("merged_at"), it.get("closed_at")
            ))
        if not rows:
            return
        with self.transaction():
            self.conn.executemany(
                f"""
                INSERT INTO {self.pull_requests_table}(repo_id, number, state, created_at, merged_at, closed_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(repo_id, number) DO UPDATE SET
                    state=excluded.state,
                    created_at=excluded.created_at,
                    merged_at=excluded.merged_at,
                    closed_at=excluded.closed_at;
                """,
                rows,
            )
