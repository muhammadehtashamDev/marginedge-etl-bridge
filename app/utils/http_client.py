import httpx
import asyncio
from app.utils.logger import logger
import time

MAX_RETRIES = 5
INITIAL_DELAY = 2  # seconds

# Track last request time globally (per process)
_last_request_time = 0

async def safe_get(client: httpx.AsyncClient, url: str, headers: dict, params: dict):
    delay = INITIAL_DELAY
    global _last_request_time

    for attempt in range(MAX_RETRIES):
        # Proactive rate limiting: ensure at least 1 second between requests
        now = time.time()
        wait_time = 1 - (now - _last_request_time)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        _last_request_time = time.time()
        try:
            response = await client.get(url, headers=headers, params=params)

            # If rate limited
            if response.status_code == 429:
                logger.warning("Rate limit hit. Sleeping...")
                await asyncio.sleep(delay)
                delay *= 2
                continue

            response.raise_for_status()
            return response

        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            logger.warning(f"Timeout. Retry {attempt+1}/{MAX_RETRIES}")
            await asyncio.sleep(delay)
            delay *= 2

        except httpx.HTTPError as e:
            logger.error(f"HTTP Error: {str(e)}")
            raise

    raise Exception("Max retries exceeded")