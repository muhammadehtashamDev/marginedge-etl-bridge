import httpx
from typing import Any, Dict, List
from app.utils.config import settings
from app.utils.logger import logger
from app.utils.http_client import safe_get

async def get_all_pages(endpoint: str, params: Dict[str, Any], data_key: str) -> List[dict]:

    all_data = []

    headers = {
        "X-Api-Key": settings.MARGIN_EDGE_API_KEY,
        "Accept": "application/json"
    }

    timeout = httpx.Timeout(120.0, connect=30.0)

    async with httpx.AsyncClient(timeout=timeout) as client:

        while True:
            url = f"{settings.BASE_URL}/{endpoint}"

            response = await safe_get(client, url, headers, params)

            json_data = response.json()
            page_data = json_data.get(data_key, [])

            all_data.extend(page_data)
            logger.info(f"{endpoint} → fetched {len(page_data)} records")

            next_page = json_data.get("nextPage")
            if next_page:
                params["nextPage"] = next_page
            else:
                break

    return all_data