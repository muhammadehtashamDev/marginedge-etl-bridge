import httpx
import asyncio
from app.utils.logger import logger
import time

INITIAL_DELAY = 2  # seconds

# Track last request time globally (per process)
_last_request_time = 0

async def safe_get(client: httpx.AsyncClient, url: str, headers: dict, params: dict):
    delay = INITIAL_DELAY
    global _last_request_time

    attempt = 0
    while True:
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
                logger.warning(f"Rate limit hit. Sleeping {delay}s before retrying (attempt {attempt+1})...")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 300)  # Cap backoff at 5 minutes
                attempt += 1
                continue

            response.raise_for_status()
            return response

        except (httpx.ReadTimeout, httpx.ConnectTimeout):
            logger.warning(f"Timeout. Sleeping {delay}s before retrying (attempt {attempt+1})...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 300)
            attempt += 1
            continue

        except httpx.HTTPError as e:
            logger.error(f"HTTP Error: {str(e)}. Sleeping {delay}s before retrying (attempt {attempt+1})...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 300)
            attempt += 1
            continue