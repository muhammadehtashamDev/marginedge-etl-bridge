import asyncio
import time
import argparse
from datetime import date, timedelta, datetime
from typing import List, Dict, Any

import httpx

from app.services.extractor import get_all_pages
from app.utils.config import settings
from app.utils.db import get_db_connection
from app.utils.http_client import safe_get
from app.utils.logger import logger
from app.services.transformer import (
    process_and_save_with_filename,
    process_and_save_order_details_with_filename,
    build_order_details_rows,
)
from app.services.loader import insert_rows
from app.utils.backup import backup_daily_files

MAX_RETRIES = 3


# -----------------------------
# Retry Helper
# -----------------------------
async def retry_request(func, retries: int = MAX_RETRIES):
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            logger.warning(f"Retry {attempt+1}/{retries} after error: {e}")
            await asyncio.sleep(wait)


# -----------------------------
# Fetch Order Detail
# -----------------------------
async def fetch_order_detail(
    client: httpx.AsyncClient,
    restaurant_id: str,
    order_id: str,
) -> Dict[str, Any] | None:

    async def _request():
        response = await safe_get(
            client,
            f"{settings.BASE_URL}/orders/{order_id}",
            headers={
                "X-Api-Key": settings.MARGIN_EDGE_API_KEY,
                "Accept": "application/json",
            },
            params={"restaurantUnitId": restaurant_id},
        )
        return response.json()

    try:
        return await retry_request(_request)
    except Exception as exc:
        logger.error(
            f"Failed order detail fetch order_id={order_id} restaurant={restaurant_id}: {exc}"
        )
        return None


# -----------------------------
# Environment & DB Check
# -----------------------------
def _check_env_and_db():
    """Validate required settings and database connectivity.

    This is primarily for running this module directly so that
    misconfiguration issues are obvious in the logs.
    """
    if not settings.MARGIN_EDGE_API_KEY:
        logger.error("MARGIN_EDGE_API_KEY is empty. Set it in .env.")
        raise RuntimeError("Missing MARGIN_EDGE_API_KEY")

    logger.info(
        "Using DB connection host=%s db=%s user=%s",
        settings.DB_HOST,
        settings.DB_NAME,
        settings.DB_USER,
    )

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        logger.info("Database connection test succeeded.")
    except Exception as exc:
        logger.error(f"Database connection test failed: {exc}")
        raise


# -----------------------------
# Main Job
# -----------------------------
async def fetch_daily_orders_and_details(target_date: date):

    start_time = time.time()

    date_str = target_date.strftime("%Y-%m-%d")

    logger.info(f"Starting daily orders job for date={date_str}")

    restaurants = await get_all_pages(
        "restaurantUnits",
        {},
        "restaurants",
    )

    if not restaurants:
        logger.info("No restaurants returned from API.")
        return

    logger.info(f"Total restaurants: {len(restaurants)}")

    all_orders: List[Dict[str, Any]] = []
    all_order_details: List[Dict[str, Any]] = []

    timeout = httpx.Timeout(120.0, connect=30.0)

    limits = httpx.Limits(
        max_keepalive_connections=10,
        max_connections=10,
    )

    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:

        for restaurant in restaurants:

            restaurant_id = restaurant.get("id")
            restaurant_name = restaurant.get("name", "unknown")

            logger.info(
                f"Processing restaurant {restaurant_id} ({restaurant_name})"
            )

            orders = await get_all_pages(
                "orders",
                {
                    "restaurantUnitId": restaurant_id,
                    "startDate": date_str,
                    "endDate": date_str,
                },
                "orders",
            )

            logger.info(
                f"Restaurant {restaurant_id} -> orders fetched: {len(orders)}"
            )

            for o in orders:
                o["restaurantUnitId"] = restaurant_id
                o["restaurantName"] = restaurant_name

            all_orders.extend(orders)

            if not orders:
                continue

            for order in orders:

                order_id = order.get("orderId") or order.get("id")

                if not order_id:
                    logger.warning(
                        f"Skipping order without id restaurant={restaurant_id}"
                    )
                    continue

                detail = await fetch_order_detail(
                    client,
                    restaurant_id,
                    order_id,
                )

                if detail is None:
                    continue

                detail.setdefault("restaurantUnitId", restaurant_id)
                detail.setdefault("restaurantName", restaurant_name)

                all_order_details.append(detail)

    orders_filename = f"orders_{date_str}.csv"
    details_filename = f"order_details_{date_str}.csv"

    if all_orders:

        orders_filepath = process_and_save_with_filename(
            all_orders,
            "orders",
            orders_filename,
        )

        # Also persist into PostgreSQL, attaching the same filename column
        # that appears in the CSV so schemas stay aligned.
        orders_rows = []
        for o in all_orders:
            row = dict(o)
            row["filename"] = orders_filename
            orders_rows.append(row)
        insert_rows("orders", orders_rows)

        logger.info(
            f"Saved {len(all_orders)} orders -> data/{orders_filename}"
        )

    else:
        logger.info("No orders collected.")

    if all_order_details:

        details_filepath = process_and_save_order_details_with_filename(
            all_order_details,
            details_filename,
        )

        # Also persist into PostgreSQL using the same flattened rows
        # that are written to the CSV so that the table matches the file.
        flattened_details = build_order_details_rows(all_order_details)
        for row in flattened_details:
            row["filename"] = details_filename
        insert_rows("order_details", flattened_details)

        logger.info(
            f"Saved {len(all_order_details)} order details -> data/{details_filename}"
        )

        # Back up generated CSVs under backup/daily/<date>/
        backup_daily_files(date_str, [p for p in [orders_filepath, details_filepath] if p])

    else:
        logger.info("No order details collected.")

    elapsed = time.time() - start_time

    # After loading into staging tables, call DB procedure to
    # move data into the main/actual schema.
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("call public.margin_edge_proc_load_data();")
        logger.info("Called public.margin_edge_proc_load_data() successfully.")
    except Exception as exc:
        logger.error(f"Failed to call public.margin_edge_proc_load_data(): {exc}")
        raise

    logger.info(
        f"Daily job completed in {elapsed:.2f} seconds"
    )


# -----------------------------
# Entry Point
# -----------------------------
def run_for_yesterday():

    target = date.today() - timedelta(days=1)
    _check_env_and_db()
    asyncio.run(fetch_daily_orders_and_details(target))


def run_for_date_range(start: date, end: date):

    if start > end:
        raise ValueError("start date must be on or before end date")

    logger.info(
        f"Running daily orders job for date range {start.isoformat()} -> {end.isoformat()}"
    )

    _check_env_and_db()

    current = start
    while current <= end:
        logger.info(f"Executing daily job for {current.isoformat()}")
        asyncio.run(fetch_daily_orders_and_details(current))
        current += timedelta(days=1)


def _parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Fetch MarginEdge daily orders and order details. "
            "If no dates are provided, runs for yesterday. "
            "Otherwise runs for the inclusive date range."
        )
    )

    parser.add_argument(
        "--start-date",
        "-s",
        help="Start date in YYYY-MM-DD format (inclusive)",
    )
    parser.add_argument(
        "--end-date",
        "-e",
        help="End date in YYYY-MM-DD format (inclusive). "
        "If omitted but --start-date is provided, the start date is used.",
    )

    return parser.parse_args()


def _parse_date(date_str: str) -> date:

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid date '{date_str}'. Expected YYYY-MM-DD.") from exc


if __name__ == "__main__":

    args = _parse_args()

    if not args.start_date and not args.end_date:
        # Default behaviour: run for yesterday when no dates are supplied.
        run_for_yesterday()
    else:
        # If only one bound is provided, treat it as a single-day run.
        if args.start_date and not args.end_date:
            start = end = _parse_date(args.start_date)
        elif args.end_date and not args.start_date:
            start = end = _parse_date(args.end_date)
        else:
            start = _parse_date(args.start_date)
            end = _parse_date(args.end_date)

        run_for_date_range(start, end)