"""
Scrapling wrapper with retry logic, proxy support, and session management.

All scrapling API calls are isolated here — update this file if the scrapling
API changes, nothing else needs to change.
"""
import logging
import time
from typing import Any, Awaitable, Callable

from .config import (
    CUSTOMS_PROXY_URL,
    SCRAPE_DELAY_SECONDS,
    SCRAPE_HEADLESS,
    SCRAPE_MAX_RETRIES,
    SCRAPE_RETRY_BASE_SECONDS,
    SCRAPE_USE_DYNAMIC,
    QUERY_ENDPOINT,
)

logger = logging.getLogger(__name__)

PageAction = Callable[[Any], Awaitable[None]]


class CustomsFetcher:
    """
    Context manager wrapping scrapling. Maintains a persistent browser session
    across many fetches to avoid per-fetch browser startup overhead.

    Usage:
        with CustomsFetcher() as fetcher:
            page = fetcher.fetch(url, page_action=my_action)
            rows = parser.parse_results_page(page, year, month)
    """

    def __init__(
        self,
        use_dynamic: bool = SCRAPE_USE_DYNAMIC,
        headless: bool = SCRAPE_HEADLESS,
    ) -> None:
        self.use_dynamic = use_dynamic
        self.headless = headless
        self._session: Any = None  # DynamicSession, opened lazily

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "CustomsFetcher":
        self._open_session()
        return self

    def __exit__(self, *args: Any) -> None:
        self._close_session()

    def _open_session(self) -> None:
        if not self.use_dynamic or self._session is not None:
            return
        try:
            from scrapling.fetchers import DynamicSession
            self._session = DynamicSession(
                headless=self.headless,
                network_idle=True,
                **self._proxy_kwargs(),
            )
            logger.debug("DynamicSession opened")
        except Exception as exc:
            logger.warning(f"Could not open DynamicSession: {exc}. Will create per-fetch.")
            self._session = None

    def _close_session(self) -> None:
        if self._session is None:
            return
        try:
            self._session.__exit__(None, None, None)
        except Exception:
            pass
        finally:
            self._session = None
            logger.debug("DynamicSession closed")

    # ── Public API ────────────────────────────────────────────────────────────

    def query_url(self, year: int, month: int) -> str:
        """Return the URL to use for a monthly export query."""
        # TODO: update after site inspection — the real URL may include
        # query parameters like ?year=2024&month=01 or be a form POST endpoint
        return QUERY_ENDPOINT

    def fetch(
        self,
        url: str,
        page_action: PageAction | None = None,
        wait_selector: str | None = None,
        extra_kwargs: dict | None = None,
    ) -> Any:
        """
        Fetch URL with exponential-backoff retry.
        Returns a scrapling Page object for CSS/XPath parsing.
        """
        kwargs = extra_kwargs or {}
        last_exc: Exception | None = None

        for attempt in range(SCRAPE_MAX_RETRIES):
            if attempt > 0:
                wait = SCRAPE_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                logger.warning(f"Retry {attempt}/{SCRAPE_MAX_RETRIES} for {url} (waiting {wait:.1f}s)")
                time.sleep(wait)
            try:
                page = self._do_fetch(url, page_action, wait_selector, kwargs)
                time.sleep(SCRAPE_DELAY_SECONDS)
                return page
            except Exception as exc:
                logger.error(f"Fetch error attempt {attempt + 1}: {exc}")
                last_exc = exc

        raise RuntimeError(
            f"All {SCRAPE_MAX_RETRIES} retries exhausted for {url}"
        ) from last_exc

    def fetch_static(self, url: str) -> Any:
        """
        Always use static (non-browser) fetcher. For known-static pages
        like /Statics/[UUID].html monthly report exports.
        """
        last_exc: Exception | None = None
        for attempt in range(SCRAPE_MAX_RETRIES):
            if attempt > 0:
                wait = SCRAPE_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
                time.sleep(wait)
            try:
                from scrapling.fetchers import Fetcher
                page = Fetcher().fetch(url, **self._proxy_kwargs())
                time.sleep(SCRAPE_DELAY_SECONDS)
                return page
            except Exception as exc:
                last_exc = exc
        raise RuntimeError(f"Static fetch exhausted retries for {url}") from last_exc

    # ── Internal ──────────────────────────────────────────────────────────────

    def _proxy_kwargs(self) -> dict:
        if CUSTOMS_PROXY_URL:
            return {"proxy": CUSTOMS_PROXY_URL}
        return {}

    def _do_fetch(
        self,
        url: str,
        page_action: PageAction | None,
        wait_selector: str | None,
        extra_kwargs: dict,
    ) -> Any:
        if not self.use_dynamic:
            from scrapling.fetchers import Fetcher
            return Fetcher().fetch(url, **self._proxy_kwargs(), **extra_kwargs)

        fetch_kwargs: dict = dict(
            headless=self.headless,
            network_idle=True,
            **self._proxy_kwargs(),
            **extra_kwargs,
        )
        if wait_selector:
            fetch_kwargs["wait_selector"] = wait_selector
        if page_action:
            fetch_kwargs["page_action"] = page_action

        if self._session is not None:
            return self._session.fetch(url, **fetch_kwargs)

        # Fallback: per-fetch DynamicFetcher (slower but works without session)
        from scrapling.fetchers import DynamicFetcher
        return DynamicFetcher().fetch(url, **fetch_kwargs)
