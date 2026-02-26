import os
from datetime import datetime
from typing import Iterable, List, Dict, Any

import pandas as pd


def _ensure_data_dir(path: str = "data") -> None:
    """
    Ensure the base data directory exists.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def process_and_save(data: Iterable[dict], resource_name: str) -> str | None:
    """
    Generic helper to flatten a list of JSON objects and save to CSV.
    Suitable for simple resources (restaurants, categories, products, vendors, orders).
    """
    data_list = list(data)
    if not data_list:
        return None

    df = pd.json_normalize(data_list)

    _ensure_data_dir()

    filename = f"data/{resource_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    return filename


def process_and_save_order_details(
    order_details: List[Dict[str, Any]],
    restaurant_id: str,
) -> str | None:
    """
    Transform a list of order-detail payloads (each containing `lineItems`)
    into a line-level dataset and save a single CSV per restaurant.

    Each output row corresponds to one line item, with order-level metadata
    (orderId, vendor, totals, etc.) repeated on each row.
    """
    if not order_details:
        return None

    # Dynamically collect all order-level fields to keep as metadata
    meta_keys = sorted(
        {
            key
            for order in order_details
            for key in order.keys()
            if key != "lineItems"
        }
    )

    # Flatten so that each line item becomes a row, with order-level metadata attached
    df = pd.json_normalize(
        order_details,
        record_path="lineItems",
        meta=meta_keys,
        errors="ignore",
    )

    _ensure_data_dir()

    filename = (
        f"data/order_details_{restaurant_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    df.to_csv(filename, index=False)
    return filename