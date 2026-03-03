import httpx
from typing import Any, Dict, List
from app.utils.config import settings
from app.utils.logger import logger
from app.utils.http_client import safe_get

async def get_all_pages(endpoint: str, params: Dict[str, Any], data_key: str) -> List[dict]:

    all_data: List[dict] = []

    headers = {
        "X-Api-Key": settings.MARGIN_EDGE_API_KEY,
        "Accept": "application/json",
    }

    timeout = httpx.Timeout(120.0, connect=30.0)

    # Work on a copy so callers' params are not mutated and we can safely
    # manage pagination state internally.
    local_params: Dict[str, Any] = dict(params)

    # Protect against buggy APIs that repeat the same nextPage token forever.
    seen_tokens = set()

    async with httpx.AsyncClient(timeout=timeout) as client:

        while True:
            url = f"{settings.BASE_URL}/{endpoint}"

            try:
                response = await safe_get(client, url, headers, local_params)
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code if exc.response is not None else None
                # For some nested endpoints (like vendorItems/packaging), a 403/404
                # can simply mean “not available here”. Treat these as a signal to
                # stop paginating instead of failing the whole ETL job.
                if status in (403, 404):
                    logger.warning(
                        f"{endpoint} -> received HTTP {status}; stopping pagination for this endpoint."
                    )
                    break
                # For other status codes, re-raise so callers see the real failure.
                raise

            json_data = response.json()
            page_data = json_data.get(data_key, [])

            all_data.extend(page_data)
            # Use ASCII-friendly log text for Windows consoles that may not support Unicode.
            logger.info(f"{endpoint} -> fetched {len(page_data)} records")

            next_page = json_data.get("nextPage")

            # Normal end of pagination
            if not next_page:
                break

            # If the API keeps returning the same nextPage token, stop to avoid an
            # infinite loop and hammering the API.
            if next_page in seen_tokens:
                logger.warning(
                    f"{endpoint} -> repeating nextPage token detected; "
                    "breaking pagination loop to avoid infinite requests."
                )
                break

            seen_tokens.add(next_page)
            local_params["nextPage"] = next_page

    return all_data