import requests
from app.utils.config import settings
from typing import Any, Dict, List

def get_all_pages(endpoint: str, params: Dict[str, Any], data_key: str) -> List[dict]:
    """
    Generic function to handle MarginEdge pagination.
    'data_key' is the JSON key like 'categories' or 'orders'.
    """
    all_data = []
    headers = {"X-Api-Key": settings.MARGIN_EDGE_API_KEY, "Accept": "application/json"}

    while True:
        response = requests.get(f"{settings.BASE_URL}/{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()

        # Add current page data to our list
        all_data.extend(json_data.get(data_key, []))

        # Check if there is a next page
        next_page = json_data.get("nextPage")
        if next_page:
            params["nextPage"] = next_page
        else:
            break

    return all_data