import asyncio

import httpx

from app.services.extractor import get_all_pages
from app.services.transformer import process_and_save, process_and_save_order_details
from app.utils.config import settings
from app.utils.http_client import safe_get
from app.utils.logger import logger

async def run_full_etl(startDate: str, endDate: str):

    summary = {
        "restaurants": 0,
        "categories": 0,
        "products": 0,
        "vendors": 0,
        "orders": 0,
        "order_details": 0
    }

    logger.info("Starting Full ETL Process")

    restaurants = await get_all_pages("restaurantUnits", {}, "restaurants")
    summary["restaurants"] = len(restaurants)
    process_and_save(restaurants, "restaurants")

    for restaurant in restaurants:
        restaurant_id = restaurant.get("id")
        logger.info(f"Processing restaurantUnitId={restaurant_id}")

        categories = await get_all_pages(
            "categories",
            {"restaurantUnitId": restaurant_id},
            "categories"
        )
        summary["categories"] += len(categories)
        process_and_save(categories, f"categories_{restaurant_id}")

        products = await get_all_pages(
            "products",
            {"restaurantUnitId": restaurant_id},
            "products"
        )
        summary["products"] += len(products)
        process_and_save(products, f"products_{restaurant_id}")

        vendors = await get_all_pages(
            "vendors",
            {"restaurantUnitId": restaurant_id},
            "vendors"
        )
        summary["vendors"] += len(vendors)
        process_and_save(vendors, f"vendors_{restaurant_id}")

        orders = await get_all_pages(
            "orders",
            {
                "restaurantUnitId": restaurant_id,
                "startDate": startDate,
                "endDate": endDate
            },
            "orders"
        )
        logger.info(
            f"restaurantUnitId={restaurant_id} -> orders fetched: {len(orders)} "
            f"(startDate={startDate}, endDate={endDate})"
        )
        summary["orders"] += len(orders)
        process_and_save(orders, f"orders_{restaurant_id}")

        if not orders:
            logger.info(
                f"restaurantUnitId={restaurant_id} -> no orders returned; skipping order details"
            )
            continue

        # Fetch all order details for this restaurant and save them in a single CSV.
        # We do this concurrently for efficiency, while still skipping bad/missing orders.
        timeout = httpx.Timeout(120.0, connect=30.0)
        limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)

        async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
            # Limit concurrency to avoid 429s / dropped connections on large restaurants.
            semaphore = asyncio.Semaphore(10)

            async def fetch_order_detail(order_id: str):
                try:
                    async with semaphore:
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
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        f"Failed to fetch order details for order_id={order_id}, "
                        f"restaurantUnitId={restaurant_id}: {exc}"
                    )
                    return None
                except httpx.HTTPError as exc:
                    # safe_get may raise other httpx errors; treat them as a per-order failure.
                    logger.error(
                        f"HTTP error while fetching order details for order_id={order_id}, "
                        f"restaurantUnitId={restaurant_id}: {exc}"
                    )
                    return None
                except httpx.RequestError as exc:
                    logger.error(
                        f"Network error while fetching order details for order_id={order_id}, "
                        f"restaurantUnitId={restaurant_id}: {exc}"
                    )
                    return None

            tasks = []

            for order in orders:
                # The paged `/orders` API returns `orderId` (not `id`) as seen in the CSV.
                # We still fall back to `id` just in case the schema changes.
                order_id = order.get("orderId") or order.get("id")

                # Some orders may come back without an ID – skip them to avoid
                # generating URLs like `/orders/None` which cause 400 errors.
                if not order_id:
                    logger.warning(
                        f"Skipping order with missing order identifier for restaurant {restaurant_id}: {order}"
                    )
                    continue

                tasks.append(fetch_order_detail(order_id))

            restaurant_order_details = [
                result for result in await asyncio.gather(*tasks) if result is not None
            ]

            summary["order_details"] += len(restaurant_order_details)
            logger.info(
                f"restaurantUnitId={restaurant_id} -> order details fetched: {len(restaurant_order_details)}"
            )

            # Save one CSV per restaurant with all of its order details,
            # flattened at the line-item level.
            if restaurant_order_details:
                process_and_save_order_details(restaurant_order_details, restaurant_id)

    logger.info("Full ETL Completed Successfully")
    return summary