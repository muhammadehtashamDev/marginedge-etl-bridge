import asyncio
import os

import httpx

from app.services.extractor import get_all_pages
from app.services.transformer import (
    expand_products_for_csv,
    expand_vendors_for_csv,
    process_and_save,
    process_and_save_order_details,
    build_order_details_rows,
)
from app.services.loader import insert_rows
from app.utils.config import settings
from app.utils.http_client import safe_get
from app.utils.logger import logger


async def run_full_etl(
    startDate: str,
    endDate: str,
    include_restaurants: bool = True,
    include_categories: bool = True,
    include_products: bool = True,
    include_vendors: bool = True,
    include_vendor_items: bool = True,
    include_vendor_packaging: bool = True,
    include_orders: bool = True,
    include_order_details: bool = True,
):

    summary = {
        "restaurants": 0,
        "categories": 0,
        "products": 0,
        "vendors": 0,
        "vendor_items": 0,
        "vendor_packaging": 0,
        "orders": 0,
        "order_details": 0,
    }

    # Global collectors so we can write ONE CSV per resource
    all_categories = []
    all_products = []
    all_vendors = []
    all_vendor_items = []
    all_vendor_packaging = []
    all_orders = []
    all_order_details = []

    logger.info("Starting Full ETL Process")

    restaurants = await get_all_pages("restaurantUnits", {}, "restaurants")
    summary["restaurants"] = len(restaurants)

    if include_restaurants:
        process_and_save(restaurants, "restaurants")

        # Also persist restaurants into PostgreSQL, ensuring each restaurant
        # is unique by its API `id` within this run. We map the API `id`
        # into a `restaurant_id` column to avoid clashing with the synthetic
        # SERIAL primary key created by the loader.
        unique_by_id = {}
        for r in restaurants:
            rid = r.get("id")
            if rid is None:
                continue
            unique_by_id[rid] = r

        if unique_by_id:
            db_rows = []
            for r in unique_by_id.values():
                row = dict(r)
                if "id" in row:
                    row["restaurant_id"] = str(row["id"])
                    del row["id"]
                db_rows.append(row)

            insert_rows("restaurants", db_rows)

    for restaurant in restaurants:
        restaurant_id = restaurant.get("id")
        restaurant_name_raw = restaurant.get("name", "unknown")
        restaurant_name = restaurant_name_raw.replace(" ", "_").replace("/", "_")
        logger.info(f"Processing restaurantUnitId={restaurant_id} ({restaurant_name})")

        categories = []
        if include_categories:
            categories = await get_all_pages(
                "categories",
                {"restaurantUnitId": restaurant_id},
                "categories",
            )
            summary["categories"] += len(categories)
            for c in categories:
                c["restaurantUnitId"] = restaurant_id
                c["restaurantName"] = restaurant_name_raw
            all_categories.extend(categories)

        products = []
        if include_products:
            products = await get_all_pages(
                "products",
                {"restaurantUnitId": restaurant_id},
                "products",
            )
            summary["products"] += len(products)
            for p in products:
                p["restaurantUnitId"] = restaurant_id
                p["restaurantName"] = restaurant_name_raw
            all_products.extend(products)

        vendors = []
        if include_vendors or include_vendor_items or include_vendor_packaging:
            vendors = await get_all_pages(
                "vendors",
                {"restaurantUnitId": restaurant_id},
                "vendors",
            )
            summary["vendors"] += len(vendors)
            if include_vendors:
                for v in vendors:
                    v["restaurantUnitId"] = restaurant_id
                    v["restaurantName"] = restaurant_name_raw
                all_vendors.extend(vendors)

        # ------------------------------------------------------------------
        # Vendor items and vendor item packaging (per restaurant).
        # ------------------------------------------------------------------
        vendor_items_all = []
        vendor_packaging_all = []

        if (include_vendor_items or include_vendor_packaging) and vendors:
            # Reuse a single HTTP client for all vendor-item packaging calls
            # to avoid creating too many connections.
            timeout = httpx.Timeout(120.0, connect=30.0)
            async with httpx.AsyncClient(timeout=timeout) as vendor_client:
                headers = {
                    "X-Api-Key": settings.MARGIN_EDGE_API_KEY,
                    "Accept": "application/json",
                }

                for vendor in vendors:
                    vendor_id = vendor.get("id") or vendor.get("vendorId")
                    if not vendor_id:
                        logger.warning(
                            f"Skipping vendor with missing identifier for restaurantUnitId={restaurant_id}: {vendor}"
                        )
                        continue

                    # Fetch all vendor items for this vendor at this restaurant
                    vendor_items = await get_all_pages(
                        f"vendors/{vendor_id}/vendorItems",
                        {"restaurantUnitId": restaurant_id},
                        "vendorItems",
                    )

                    for item in vendor_items:
                        # Ensure some useful keys are present for downstream analysis
                        item.setdefault("restaurantUnitId", restaurant_id)
                        item.setdefault("vendorId", vendor_id)
                        item.setdefault("restaurantName", restaurant_name_raw)

                    vendor_items_all.extend(vendor_items)

                    # For each vendor item, fetch its packaging definitions.
                    # The packaging endpoint is very small per item in practice, and
                    # its pagination can behave oddly. To keep things robust and
                    # avoid noisy loops, we only fetch the *first* page and ignore
                    # any nextPage token.
                    if not include_vendor_packaging:
                        continue
                    for item in vendor_items:
                        vendor_item_code = item.get("vendorItemCode")
                        if not vendor_item_code:
                            continue

                        url = (
                            f"{settings.BASE_URL}/vendors/{vendor_id}/vendorItems/"
                            f"{vendor_item_code}/packaging"
                        )
                        try:
                            response = await safe_get(
                                vendor_client,
                                url,
                                headers,
                                {"restaurantUnitId": restaurant_id},
                            )
                        except httpx.HTTPStatusError as exc:
                            status = (
                                exc.response.status_code
                                if exc.response is not None
                                else None
                            )
                            if status in (403, 404):
                                logger.warning(
                                    f"{url} -> HTTP {status}; skipping packaging for this vendor item."
                                )
                                continue
                            raise

                        data = response.json()
                        packagings = data.get("packagings", []) or []

                        for pkg in packagings:
                            pkg.setdefault("restaurantUnitId", restaurant_id)
                            pkg.setdefault("vendorId", vendor_id)
                            pkg.setdefault("vendorItemCode", vendor_item_code)
                            pkg.setdefault("restaurantName", restaurant_name_raw)

                        vendor_packaging_all.extend(packagings)

        summary["vendor_items"] += len(vendor_items_all)
        summary["vendor_packaging"] += len(vendor_packaging_all)

        if include_vendor_items and vendor_items_all:
            all_vendor_items.extend(vendor_items_all)

        if include_vendor_packaging and vendor_packaging_all:
            all_vendor_packaging.extend(vendor_packaging_all)

        orders = []
        if include_orders or include_order_details:
            orders = await get_all_pages(
                "orders",
                {
                    "restaurantUnitId": restaurant_id,
                    "startDate": startDate,
                    "endDate": endDate,
                },
                "orders",
            )
            logger.info(
                f"restaurantUnitId={restaurant_id} -> orders fetched: {len(orders)} "
                f"(startDate={startDate}, endDate={endDate})"
            )
            summary["orders"] += len(orders)

            if include_orders:
                for o in orders:
                    o["restaurantUnitId"] = restaurant_id
                    o["restaurantName"] = restaurant_name_raw
                all_orders.extend(orders)

        if not include_order_details:
            # Skip fetching order details entirely
            continue

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

            if restaurant_order_details:
                for od in restaurant_order_details:
                    od.setdefault("restaurantUnitId", restaurant_id)
                    od.setdefault("restaurantName", restaurant_name_raw)
                all_order_details.extend(restaurant_order_details)

    # After processing all restaurants, write ONE CSV per resource (if enabled)
    if include_categories and all_categories:
        process_and_save(all_categories, "categories")
        insert_rows("categories", all_categories)

    if include_products and all_products:
        product_rows = expand_products_for_csv(all_products)
        process_and_save(product_rows, "products")
        insert_rows("products", product_rows)

    if include_vendors and all_vendors:
        vendor_rows = expand_vendors_for_csv(all_vendors)
        process_and_save(vendor_rows, "vendors")
        insert_rows("vendors", vendor_rows)

    if include_vendor_items and all_vendor_items:
        process_and_save(all_vendor_items, "vendor_items")
        insert_rows("vendor_items", all_vendor_items)

    if include_vendor_packaging and all_vendor_packaging:
        process_and_save(all_vendor_packaging, "vendor_packaging")
        insert_rows("vendor_packaging", all_vendor_packaging)

    if include_orders and all_orders:
        orders_filepath = process_and_save(all_orders, "orders")
        orders_filename = os.path.basename(orders_filepath) if orders_filepath else None
        orders_rows = []
        for o in all_orders:
            row = dict(o)
            if orders_filename is not None:
                row["filename"] = orders_filename
            orders_rows.append(row)
        insert_rows("orders", orders_rows)

    if include_order_details and all_order_details:
        # Build the same flattened, line-level rows used for the CSV so that
        # the database table matches the CSV contents row-for-row.
        order_details_filepath = process_and_save_order_details(
            all_order_details, "order_details"
        )
        order_details_filename = (
            os.path.basename(order_details_filepath)
            if order_details_filepath
            else None
        )
        flattened_order_details = build_order_details_rows(all_order_details)
        if order_details_filename is not None:
            for row in flattened_order_details:
                row["filename"] = order_details_filename
        insert_rows("order_details", flattened_order_details)

    logger.info("Full ETL Completed Successfully")
    return summary