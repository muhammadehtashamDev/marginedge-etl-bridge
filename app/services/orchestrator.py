from app.services.extractor import get_all_pages
from app.services.transformer import process_and_save
from app.utils.logger import logger
import httpx
from app.utils.config import settings

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
        summary["orders"] += len(orders)
        process_and_save(orders, f"orders_{restaurant_id}")

        async with httpx.AsyncClient() as client:
            for order in orders:
                order_id = order.get("id")

                # Some orders may come back without an ID – skip them to avoid
                # generating URLs like `/orders/None` which cause 400 errors.
                if not order_id:
                    logger.warning(
                        f"Skipping order with missing 'id' for restaurant {restaurant_id}: {order}"
                    )
                    continue

                try:
                    response = await client.get(
                        f"{settings.BASE_URL}/orders/{order_id}",
                        headers={
                            "X-Api-Key": settings.MARGIN_EDGE_API_KEY,
                            "Accept": "application/json"
                        },
                        params={"restaurantUnitId": restaurant_id}
                    )
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        f"Failed to fetch order details for order_id={order_id}, "
                        f"restaurantUnitId={restaurant_id}: {exc}"
                    )
                    # Skip this order but continue processing the rest
                    continue

                process_and_save([response.json()], f"order_detail_{order_id}")
                summary["order_details"] += 1

    logger.info("Full ETL Completed Successfully")
    return summary