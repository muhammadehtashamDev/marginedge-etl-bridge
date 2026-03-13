import asyncio
import time
from datetime import date, timedelta

from app.services.orchestrator import run_full_etl
from app.utils.logger import logger


# -----------------------------
# Configuration
# -----------------------------
MAX_RETRIES = 3
API_DELAY_SECONDS = 0.3


# -----------------------------
# Retry Helper
# -----------------------------
async def retry_async(func, retries=MAX_RETRIES):

    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:

            if attempt == retries - 1:
                raise

            wait = 2 ** attempt
            logger.warning(
                f"[weekly-full-sync] Retry {attempt+1}/{retries} after error: {e}"
            )

            await asyncio.sleep(wait)


# -----------------------------
# Previous Week Range
# -----------------------------
def _previous_week_range(today: date) -> tuple[date, date]:

    end = today - timedelta(days=1)
    start = end - timedelta(days=6)

    return start, end


# -----------------------------
# Run Weekly ETL
# -----------------------------
async def run_weekly_full_sync_for_range(start: date, end: date):

    start_time = time.time()

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    logger.info(
        f"[weekly-full-sync] Starting weekly full sync "
        f"{start_str} -> {end_str}"
    )

    async def run_job():

        await run_full_etl(
            startDate=start_str,
            endDate=end_str,
            include_restaurants=True,
            include_categories=True,
            include_products=True,
            include_vendors=True,
            include_vendor_items=True,
            include_vendor_packaging=False,
            include_orders=True,
            include_order_details=True,
        )

    await retry_async(run_job)

    # small delay to protect API
    await asyncio.sleep(API_DELAY_SECONDS)

    elapsed = time.time() - start_time

    logger.info(
        f"[weekly-full-sync] Completed weekly sync "
        f"{start_str} -> {end_str} in {elapsed:.2f}s"
    )


# -----------------------------
# Entry Point
# -----------------------------
def run_for_previous_week():

    today = date.today()

    start, end = _previous_week_range(today)

    asyncio.run(run_weekly_full_sync_for_range(start, end))


if __name__ == "__main__":
    run_for_previous_week()