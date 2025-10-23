from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    target: str
    target_type: str
    token: Optional[str]
    db_path: str = "data.db"
    include_contributors: bool = False
    concurrency: int = 10
    max_repos: Optional[int] = None
    include_dora: bool = False
    since_iso: Optional[str] = None
    max_prs_per_repo: Optional[int] = None

    @staticmethod
    def from_env() -> "Config":
        target = os.getenv("GITHUB_TARGET", "")
        target_type = os.getenv("GITHUB_TARGET_TYPE", "user")
        token = os.getenv("GITHUB_TOKEN")
        if token is not None:
            token = token.strip()
        db_path = os.getenv("DB_PATH", "data.db")
        include_contributors = os.getenv("INCLUDE_CONTRIBUTORS", "0").lower() in {"1", "true", "yes"}
        include_dora = os.getenv("INCLUDE_DORA", "0").lower() in {"1", "true", "yes"}
        concurrency = int(os.getenv("CONCURRENCY", "10"))
        max_repos_env = os.getenv("MAX_REPOS")
        max_repos = int(max_repos_env) if max_repos_env else None
        since_iso = os.getenv("SINCE")
        max_prs_env = os.getenv("MAX_PRS_PER_REPO")
        max_prs_per_repo = int(max_prs_env) if max_prs_env else None
        return Config(
            target=target,
            target_type=target_type,
            token=token,
            db_path=db_path,
            include_contributors=include_contributors,
            concurrency=concurrency,
            max_repos=max_repos,
            include_dora=include_dora,
            since_iso=since_iso,
            max_prs_per_repo=max_prs_per_repo,
        )
