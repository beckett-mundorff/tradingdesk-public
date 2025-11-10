"""Kalshi social API client for fetching user holdings."""

import time
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class KalshiSocialClient:
    """Client for Kalshi social/profile API endpoints."""

    def __init__(
        self,
        base_url: str = "https://api.elections.kalshi.com/v1",
        timeout: int = 10,
        max_retries: int = 5,
        backoff_factor: float = 0.5,
        verbose: bool = False,
        logger=None,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.verbose = verbose
        self.logger = logger or print

        # Create session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": "KalshiMonitor/1.0"})

    def _log(self, message: str):
        """Log message if verbose enabled."""
        if self.verbose and self.logger:
            self.logger(message)

    def fetch_user_holdings(
        self,
        nickname: str,
        limit: int = 20,
        closed_positions: bool = False,
        cursor: Optional[str] = None,
    ) -> Dict:
        """
        Fetch user holdings from Kalshi social API.

        Args:
            nickname: User nickname
            limit: Number of holdings to fetch (default: 20, max: 100)
            closed_positions: Whether to include closed positions
            cursor: Pagination cursor

        Returns:
            Dict containing holdings, cursor, and social_id
        """
        url = f"{self.base_url}/social/profile/holdings"
        
        params = {
            "nickname": nickname,
            "limit": min(limit, 100),
            "closed_positions": str(closed_positions).lower(),
        }
        
        if cursor:
            params["cursor"] = cursor

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self._log(f"[ERROR] Failed to fetch holdings for {nickname}: {e}")
            raise

    def fetch_all_holdings(
        self,
        nickname: str,
        closed_positions: bool = False,
        max_pages: int = 10,
    ) -> List[Dict]:
        """
        Fetch all holdings for a user (with pagination).

        Args:
            nickname: User nickname
            closed_positions: Whether to include closed positions
            max_pages: Maximum number of pages to fetch

        Returns:
            List of all holdings
        """
        all_holdings = []
        cursor = None
        page = 0

        while page < max_pages:
            try:
                data = self.fetch_user_holdings(
                    nickname=nickname,
                    limit=100,
                    closed_positions=closed_positions,
                    cursor=cursor,
                )

                holdings = data.get("holdings", [])
                if not holdings:
                    break

                all_holdings.extend(holdings)
                
                cursor = data.get("cursor")
                if not cursor:
                    break

                page += 1
                time.sleep(0.1)  # Rate limiting

            except Exception as e:
                self._log(f"[ERROR] Failed to fetch page {page + 1}: {e}")
                break

        return all_holdings

    def validate_nickname(self, nickname: str) -> bool:
        """
        Validate that a nickname exists and is accessible.

        Args:
            nickname: User nickname to validate

        Returns:
            True if nickname is valid and accessible
        """
        try:
            data = self.fetch_user_holdings(nickname, limit=1)
            return "holdings" in data or "social_id" in data
        except Exception:
            return False

    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
