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
    filename_prefix: str,
) -> str | None:
    """
    Transform a list of order-detail payloads (each containing `lineItems`)
    into a line-level dataset and save to a single CSV.

    Each output row corresponds to one line item, with order-level metadata
    (orderId, vendor, totals, etc.) repeated on each row.
    """
    if not order_details:
        return None

    # Build rows manually so that orders with empty/missing `lineItems` still appear
    # (at least one row per order).
    rows: List[Dict[str, Any]] = []

    for order in order_details:
        order_meta = {k: v for k, v in order.items() if k != "lineItems"}

        # Helpful derived fields for auditing/completeness checks
        attachments = order_meta.get("attachments")
        if isinstance(attachments, list):
            order_meta["attachments_count"] = len(attachments)
        else:
            order_meta["attachments_count"] = 0 if attachments is None else None

        line_items = order.get("lineItems") or []
        if not isinstance(line_items, list):
            line_items = []

        order_meta["lineItems_count"] = len(line_items)

        if line_items:
            for idx, item in enumerate(line_items):
                row = dict(order_meta)
                if isinstance(item, dict):
                    # Prefix line-item fields to avoid collisions with order-level keys
                    row.update({f"lineItem_{k}": v for k, v in item.items()})
                row["lineItem_index"] = idx
                rows.append(row)
        else:
            # Still include the order even if it has no line items
            row = dict(order_meta)
            row["lineItem_index"] = None
            rows.append(row)

    df = pd.DataFrame(rows)

    _ensure_data_dir()

    filename = (
        f"data/{filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    df.to_csv(filename, index=False)
    return filename