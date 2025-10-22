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

    @staticmethod
    def from_env() -> "Config":
        target = os.getenv("GITHUB_TARGET", "")
        target_type = os.getenv("GITHUB_TARGET_TYPE", "user")
        token = os.getenv("GITHUB_TOKEN")
        db_path = os.getenv("DB_PATH", "data.db")
        include_contributors = os.getenv("INCLUDE_CONTRIBUTORS", "0").lower() in {"1", "true", "yes"}
        concurrency = int(os.getenv("CONCURRENCY", "10"))
        max_repos_env = os.getenv("MAX_REPOS")
        max_repos = int(max_repos_env) if max_repos_env else None
        return Config(
            target=target,
            target_type=target_type,
            token=token,
            db_path=db_path,
            include_contributors=include_contributors,
            concurrency=concurrency,
            max_repos=max_repos,
        )
