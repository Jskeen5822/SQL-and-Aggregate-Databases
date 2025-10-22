from __future__ import annotations

import argparse
import asyncio
from typing import List, Dict, Any, Tuple

from .config import Config
from .db import Database
from .github_client import GitHubClient
from .aggregator import compute_aggregates


def parse_args() -> Tuple[Config, bool]:
    p = argparse.ArgumentParser(description="Fetch GitHub repositories and compute aggregates")
    target = p.add_mutually_exclusive_group(required=False)
    target.add_argument("--user", help="GitHub username to fetch repos for")
    target.add_argument("--org", help="GitHub organization to fetch repos for")
    p.add_argument("--token", help="GitHub token (or set GITHUB_TOKEN)")
    p.add_argument("--db", default="data.db", help="SQLite database path (default: data.db)")
    p.add_argument("--include-contributors", action="store_true", help="Fetch & store contributor counts")
    p.add_argument("--concurrency", type=int, default=10, help="Max parallel requests (default: 10)")
    p.add_argument("--max-repos", type=int, help="Optional limit on number of repos to process")
    p.add_argument("--skip-aggregates", action="store_true", help="Skip computing aggregates")

    args = p.parse_args()

    env_cfg = Config.from_env()
    if args.user:
        tgt = args.user
        tgt_type = "user"
    elif args.org:
        tgt = args.org
        tgt_type = "org"
    else:
        tgt = env_cfg.target
        tgt_type = env_cfg.target_type

    if not tgt:
        p.error("You must provide --user or --org (or set GITHUB_TARGET and GITHUB_TARGET_TYPE)")

    token = args.token or env_cfg.token

    return Config(
        target=tgt,
        target_type=tgt_type,
        token=token,
        db_path=args.db or env_cfg.db_path,
        include_contributors=bool(args.include_contributors or env_cfg.include_contributors),
        concurrency=int(args.concurrency or env_cfg.concurrency),
        max_repos=args.max_repos if args.max_repos is not None else env_cfg.max_repos,
    ), bool(args.skip_aggregates)


async def fetch_and_store(cfg: Config, skip_aggregates: bool = False) -> None:
    db = Database(cfg.db_path)
    db.init_schema()

    async with GitHubClient(token=cfg.token, concurrency=cfg.concurrency) as gh:
        # Fetch repositories
        if cfg.target_type == "org":
            repos = await gh.list_org_repos(cfg.target)
        else:
            repos = await gh.list_user_repos(cfg.target)

        if cfg.max_repos:
            repos = repos[: cfg.max_repos]

        # Insert repositories first
        db.upsert_repositories(repos)

        # Fetch languages (and contributors optionally) concurrently
        async def fetch_repo_extras(repo: Dict[str, Any]) -> Dict[str, Any]:
            owner = repo["owner"]["login"]
            name = repo["name"]
            rid = int(repo["id"])  # repo_id
            langs = await gh.get_repo_languages(owner, name)
            data: Dict[str, Any] = {"repo_id": rid, "languages": langs}
            if cfg.include_contributors:
                contrib = await gh.list_repo_contributors(owner, name)
                # normalize only needed fields
                norm = [{"login": c.get("login"), "contributions": c.get("contributions", 0)} for c in contrib]
                data["contributors"] = norm
            return data

        # Limit batch size to avoid blasting API too hard
        tasks = [fetch_repo_extras(r) for r in repos]
        results: List[Dict[str, Any]] = []
        for i in range(0, len(tasks), 50):
            batch = tasks[i : i + 50]
            results.extend(await asyncio.gather(*batch))

        # Split and save
        lang_items = [{"repo_id": it["repo_id"], "languages": it["languages"]} for it in results]
        db.upsert_languages(lang_items)

        if cfg.include_contributors:
            contrib_items = [
                {"repo_id": it["repo_id"], "contributors": it.get("contributors", [])}
                for it in results
            ]
            db.upsert_contributors(contrib_items)

        # Compute aggregates
        if not skip_aggregates:
            compute_aggregates(db)

        db.close()


def main() -> None:
    cfg, skip_aggs = parse_args()
    asyncio.run(fetch_and_store(cfg, skip_aggregates=skip_aggs))


if __name__ == "__main__":
    main()
