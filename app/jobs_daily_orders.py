import asyncio
import time
from datetime import date, timedelta
from typing import List, Dict, Any

import httpx

from app.services.extractor import get_all_pages
from app.utils.config import settings
from app.utils.http_client import safe_get
from app.utils.logger import logger
from app.services.transformer import (
    process_and_save_with_filename,
    process_and_save_order_details_with_filename,
)

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

        process_and_save_with_filename(
            all_orders,
            "orders",
            orders_filename,
        )

        logger.info(
            f"Saved {len(all_orders)} orders -> data/{orders_filename}"
        )

    else:
        logger.info("No orders collected.")

    if all_order_details:

        process_and_save_order_details_with_filename(
            all_order_details,
            details_filename,
        )

        logger.info(
            f"Saved {len(all_order_details)} order details -> data/{details_filename}"
        )

    else:
        logger.info("No order details collected.")

    elapsed = time.time() - start_time

    logger.info(
        f"Daily job completed in {elapsed:.2f} seconds"
    )


# -----------------------------
# Entry Point
# -----------------------------
def run_for_yesterday():

    target = date.today() - timedelta(days=1)

    asyncio.run(fetch_daily_orders_and_details(target))


if __name__ == "__main__":
    run_for_yesterday()