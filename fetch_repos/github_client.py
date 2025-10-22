from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
import aiohttp
import time

API_ROOT = "https://api.github.com"


class RateLimitError(Exception):
    pass


class GitHubClient:
    def __init__(self, token: Optional[str] = None, concurrency: int = 10, timeout: int = 30) -> None:
        self.token = token
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "GitHubClient":
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "repo-aggregator/0.1",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        self._session = aiohttp.ClientSession(headers=headers, timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _request_json(self, method: str, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        assert self._session is not None
        backoff = 1.0
        while True:
            async with self.semaphore:
                async with self._session.request(method, url, params=params) as resp:
                    if resp.status == 403:
                        # Could be rate limit
                        remaining = resp.headers.get("X-RateLimit-Remaining")
                        reset = resp.headers.get("X-RateLimit-Reset")
                        if remaining == "0" and reset:
                            reset_ts = int(reset)
                            sleep_for = max(0, reset_ts - int(time.time()) + 1)
                            await asyncio.sleep(sleep_for)
                            continue
                    if resp.status in (429, 502, 503, 504):
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 30)
                        continue
                    resp.raise_for_status()
                    return await resp.json()

    async def _paginate(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        page = 1
        while True:
            merged = dict(params or {})
            merged.update({"per_page": 100, "page": page})
            data = await self._request_json("GET", url, params=merged)
            if not isinstance(data, list):
                # some endpoints might return a dict; normalize
                page_items = data.get("items", []) if isinstance(data, dict) else []
            else:
                page_items = data
            if not page_items:
                break
            items.extend(page_items)
            page += 1
        return items

    async def list_user_repos(self, username: str) -> List[Dict[str, Any]]:
        url = f"{API_ROOT}/users/{username}/repos"
        return await self._paginate(url, params={"type": "all", "sort": "updated"})

    async def list_org_repos(self, org: str) -> List[Dict[str, Any]]:
        url = f"{API_ROOT}/orgs/{org}/repos"
        return await self._paginate(url, params={"type": "all", "sort": "updated"})

    async def get_repo_languages(self, owner: str, repo: str) -> Dict[str, int]:
        url = f"{API_ROOT}/repos/{owner}/{repo}/languages"
        data = await self._request_json("GET", url)
        if isinstance(data, dict):
            return {k: int(v) for k, v in data.items()}
        return {}

    async def list_repo_contributors(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        url = f"{API_ROOT}/repos/{owner}/{repo}/contributors"
        return await self._paginate(url)
