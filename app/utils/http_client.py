import httpx
import asyncio
from app.utils.logger import logger

MAX_RETRIES = 5
INITIAL_DELAY = 2  # seconds

async def safe_get(client: httpx.AsyncClient, url: str, headers: dict, params: dict):
    delay = INITIAL_DELAY

    for attempt in range(MAX_RETRIES):
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

        except httpx.ReadTimeout:
            logger.warning(f"ReadTimeout. Retry {attempt+1}/{MAX_RETRIES}")
            await asyncio.sleep(delay)
            delay *= 2

        except httpx.HTTPError as e:
            logger.error(f"HTTP Error: {str(e)}")
            raise

    raise Exception("Max retries exceeded")