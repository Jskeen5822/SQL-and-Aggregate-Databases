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
    p.add_argument("--include-dora", action="store_true", help="Fetch DORA-related data (commits, PRs)")
    p.add_argument("--since", help="Optional ISO date (YYYY-MM-DD) to filter PRs by created_at")
    p.add_argument("--max-prs-per-repo", type=int, help="Limit number of PRs fetched per repo")

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

    token = (args.token.strip() if isinstance(args.token, str) else None) or env_cfg.token

    return Config(
        target=tgt,
        target_type=tgt_type,
    token=token,
        db_path=args.db or env_cfg.db_path,
        include_contributors=bool(args.include_contributors or env_cfg.include_contributors),
        concurrency=int(args.concurrency or env_cfg.concurrency),
        max_repos=args.max_repos if args.max_repos is not None else env_cfg.max_repos,
        include_dora=bool(args.include_dora or env_cfg.include_dora),
        since_iso=args.since or env_cfg.since_iso,
        max_prs_per_repo=args.max_prs_per_repo if args.max_prs_per_repo is not None else env_cfg.max_prs_per_repo,
    ), bool(args.skip_aggregates)


def _namespace_from_target(target: str) -> str:
    import re
    return re.sub(r"[^0-9A-Za-z_]+", "_", target).strip("_").lower()[:40] or "default"


async def fetch_and_store(cfg: Config, skip_aggregates: bool = False) -> None:
    ns = _namespace_from_target(cfg.target)
    db = Database(cfg.db_path, namespace=ns)
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
        if lang_items:
            db.upsert_languages(lang_items)

        if cfg.include_contributors:
            contrib_items = [
                {"repo_id": it["repo_id"], "contributors": it.get("contributors", [])}
                for it in results
            ]
            if contrib_items:
                db.upsert_contributors(contrib_items)

        # Fetch DORA datasets (commit activity + PRs)
        if cfg.include_dora:
            from datetime import datetime
            since_dt = None
            if cfg.since_iso:
                try:
                    since_dt = datetime.fromisoformat(cfg.since_iso)
                except ValueError:
                    since_dt = None

            async def fetch_commit_activity(repo: Dict[str, Any]):
                owner = repo["owner"]["login"]
                name = repo["name"]
                rid = int(repo["id"])
                weeks = await gh.get_repo_commit_activity(owner, name)
                items = []
                for w in weeks:
                    # w: {week: epoch_seconds, total: int}
                    try:
                        import datetime as _dt
                        dt = _dt.datetime.utcfromtimestamp(int(w.get("week", 0)))
                        week_start = dt.date().isoformat()
                        items.append({"repo_id": rid, "week_start": week_start, "total": int(w.get("total", 0))})
                    except Exception:
                        continue
                return items

            async def fetch_pull_requests(repo: Dict[str, Any]):
                owner = repo["owner"]["login"]
                name = repo["name"]
                rid = int(repo["id"])
                prs = await gh.list_repo_pull_requests(owner, name, state="all")
                # Optional filter by since date
                if since_dt:
                    prs = [p for p in prs if p.get("created_at") and p["created_at"] >= cfg.since_iso]
                # Optional cap per repo
                if cfg.max_prs_per_repo:
                    prs = prs[: cfg.max_prs_per_repo]
                # Normalize fields we need
                norm = [
                    {
                        "repo_id": rid,
                        "number": p.get("number"),
                        "state": p.get("state"),
                        "created_at": p.get("created_at"),
                        "merged_at": p.get("merged_at"),
                        "closed_at": p.get("closed_at"),
                    }
                    for p in prs
                ]
                return norm

            # Commit activity tasks
            ca_tasks = [fetch_commit_activity(r) for r in repos]
            ca_results: List[List[Dict[str, Any]]] = []
            for i in range(0, len(ca_tasks), 50):
                batch = ca_tasks[i : i + 50]
                ca_results.extend(await asyncio.gather(*batch))
            flat_ca = [item for sub in ca_results for item in sub]
            if flat_ca:
                db.upsert_commit_activity(flat_ca)

            # PR tasks
            pr_tasks = [fetch_pull_requests(r) for r in repos]
            pr_results: List[List[Dict[str, Any]]] = []
            for i in range(0, len(pr_tasks), 25):
                batch = pr_tasks[i : i + 25]
                pr_results.extend(await asyncio.gather(*batch))
            flat_prs = [item for sub in pr_results for item in sub]
            if flat_prs:
                db.upsert_pull_requests(flat_prs)

        # Compute aggregates
        if not skip_aggregates:
            compute_aggregates(db)

        db.close()


def main() -> None:
    cfg, skip_aggs = parse_args()
    asyncio.run(fetch_and_store(cfg, skip_aggregates=skip_aggs))


if __name__ == "__main__":
    main()
