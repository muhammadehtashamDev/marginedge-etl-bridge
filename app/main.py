from fastapi import FastAPI, BackgroundTasks, Query, HTTPException, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.openapi.docs import get_swagger_ui_html
import secrets
import asyncio
from app.services.orchestrator import run_full_etl
from app.utils.logger import logger
from app.utils.config import settings

app = FastAPI(
    title="MarginEdge ETL Master",
    version="2.0",
    description="Production Level ETL Orchestrator"
)

# --- AUTH SETUP ---
security = HTTPBasic()
AUTHORIZED_USERS = {
    settings.ADMIN_USERNAME: settings.ADMIN_PASSWORD
}

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    correct_password = AUTHORIZED_USERS.get(credentials.username)
    if not correct_password or not secrets.compare_digest(credentials.password, correct_password):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

# --- CONCURRENCY LOCK ---
etl_lock = asyncio.Lock()

@app.post("/sync/full", tags=["Master ETL"])
async def sync_full(
    background_tasks: BackgroundTasks,
    startDate: str = Query(..., description="Start Date (YYYY-MM-DD)"),
    endDate: str = Query(..., description="End Date (YYYY-MM-DD)"),
    include_restaurants: bool = Query(
        True, description="Include restaurant units CSV"
    ),
    include_categories: bool = Query(
        True, description="Include categories CSVs per restaurant"
    ),
    include_products: bool = Query(
        True, description="Include products CSVs per restaurant"
    ),
    include_vendors: bool = Query(
        True, description="Include vendors CSVs per restaurant"
    ),
    include_vendor_items: bool = Query(
        True, description="Include vendor items CSVs per restaurant"
    ),
    include_vendor_packaging: bool = Query(
        True, description="Include vendor item packaging CSVs per restaurant"
    ),
    include_orders: bool = Query(
        True, description="Include orders CSVs per restaurant"
    ),
    include_order_details: bool = Query(
        True, description="Include order-details CSVs per restaurant"
    ),
    user: str = Depends(authenticate),
):
    if etl_lock.locked():
        raise HTTPException(status_code=429, detail="ETL process already running. Please wait until it completes.")
    try:
        async def locked_etl():
            async with etl_lock:
                await run_full_etl(
                    startDate=startDate,
                    endDate=endDate,
                    include_restaurants=include_restaurants,
                    include_categories=include_categories,
                    include_products=include_products,
                    include_vendors=include_vendors,
                    include_vendor_items=include_vendor_items,
                    include_vendor_packaging=include_vendor_packaging,
                    include_orders=include_orders,
                    include_order_details=include_order_details,
                )
        background_tasks.add_task(locked_etl)
        logger.info(f"ETL Job Triggered via API by {user}")
        return {
            "status": "ETL Job Started",
            "message": "Process running in background. Check logs for progress."
        }
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

# --- PROTECT SWAGGER UI ---
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui(credentials: HTTPBasicCredentials = Depends(security)):
    authenticate(credentials)
    return get_swagger_ui_html(openapi_url=app.openapi_url, title=app.title + " - Swagger UI")