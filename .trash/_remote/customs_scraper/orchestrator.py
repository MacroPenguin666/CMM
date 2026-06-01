"""
Top-level scrape loop. Coordinates fetcher, parser, DB writes, and checkpointing.

Iteration strategy:
  Tier 1 (preferred): Query by HS8 code → site returns all destination countries
                       in one paginated result set. ~9000 queries per month.
  Tier 2 (fallback):  Site requires country selection → iterate HS8 × country.
                       Falls back automatically if Tier 1 yields no country column.
"""
import logging

from .config import DB_PATH
from .countries import load_countries
from .db import (
    checkpoint_done,
    finish_run,
    get_completed_hs_codes,
    start_run,
    upsert_export_rows,
)
from .fetcher import CustomsFetcher
from .hs_codes import load_hs_codes
from .page_actions import build_hs_query_action, click_next_page_action
from .parser import has_next_page, parse_results_page

logger = logging.getLogger(__name__)


class ScrapeOrchestrator:
    """
    Run a complete monthly scrape, with checkpoint/resume support.

    Args:
        year:           Year to scrape.
        month:          Month to scrape (1–12).
        resume_run_id:  If given, resume this partial run (skips completed HS codes).
        db_path:        Override SQLite path (default from config).
    """

    def __init__(
        self,
        year: int,
        month: int,
        resume_run_id: str | None = None,
        db_path: str = DB_PATH,
    ) -> None:
        self.year = year
        self.month = month
        self.resume_run_id = resume_run_id
        self.db_path = db_path
        self._rows_inserted = 0
        self._rows_updated = 0
        self._hs_codes_done = 0

    def run(self) -> str:
        """
        Execute the full monthly scrape.
        Returns final status string: 'success' | 'partial' | 'failed'.
        """
        all_hs_codes = load_hs_codes()
        _countries = load_countries()  # loaded for Tier-2 fallback, not used in Tier-1

        if self.resume_run_id:
            run_id = self.resume_run_id
            completed = get_completed_hs_codes(run_id, self.db_path)
            hs_codes = [c for c in all_hs_codes if c["code"] not in completed]
            logger.info(
                f"Resuming run {run_id}: {len(hs_codes)} HS codes remaining "
                f"(skipping {len(completed)} already done)"
            )
        else:
            run_id = start_run(self.year, self.month, len(all_hs_codes), self.db_path)
            hs_codes = all_hs_codes
            logger.info(
                f"Started run {run_id} for {self.year}-{self.month:02d}, "
                f"{len(hs_codes)} HS codes"
            )

        status = "success"
        failed_codes: list[str] = []

        try:
            with CustomsFetcher() as fetcher:
                for i, hs in enumerate(hs_codes):
                    try:
                        self._scrape_hs_code(fetcher, run_id, hs)
                        self._hs_codes_done += 1
                        logger.info(
                            f"[{i + 1}/{len(hs_codes)}] HS {hs['code']} done "
                            f"(+{self._rows_inserted} inserted, +{self._rows_updated} updated total)"
                        )
                    except Exception as exc:
                        logger.error(f"Failed HS {hs['code']}: {exc}", exc_info=True)
                        failed_codes.append(hs["code"])
                        status = "partial"
                        # Continue — checkpoint not written, so --resume will retry this code

        except Exception as exc:
            logger.error(f"Scrape run {run_id} aborted: {exc}", exc_info=True)
            finish_run(
                run_id, "failed", self._rows_inserted, self._rows_updated,
                self._hs_codes_done, str(exc), self.db_path,
            )
            raise

        if failed_codes:
            logger.warning(f"Run {run_id} partial: {len(failed_codes)} HS codes failed: {failed_codes}")

        finish_run(
            run_id, status, self._rows_inserted, self._rows_updated,
            self._hs_codes_done, None, self.db_path,
        )
        logger.info(
            f"Run {run_id} finished: status={status}, "
            f"inserted={self._rows_inserted}, updated={self._rows_updated}"
        )
        return status

    def _scrape_hs_code(
        self,
        fetcher: CustomsFetcher,
        run_id: str,
        hs: dict,
    ) -> None:
        """
        Fetch all paginated results for one HS8 code and upsert to DB.
        Checkpoints the HS code as done when complete.
        """
        all_rows: list[dict] = []

        action = build_hs_query_action(
            year=self.year,
            month=self.month,
            hs8_code=hs["code"],
        )
        page = fetcher.fetch(
            url=fetcher.query_url(self.year, self.month),
            page_action=action,
        )

        page_num = 1
        while True:
            rows = parse_results_page(page, self.year, self.month)
            all_rows.extend(rows)
            logger.debug(f"  HS {hs['code']} page {page_num}: {len(rows)} rows")

            if not has_next_page(page):
                break

            page_num += 1
            next_action = click_next_page_action(page_num)
            page = fetcher.fetch(
                url=fetcher.query_url(self.year, self.month),
                page_action=next_action,
            )

        ins, upd = upsert_export_rows(all_rows, self.db_path)
        self._rows_inserted += ins
        self._rows_updated += upd

        # country_code=None means "all countries for this HS code are done"
        checkpoint_done(run_id, hs["code"], None, len(all_rows), self.db_path)
