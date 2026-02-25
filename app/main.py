import requests
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from app.services.extractor import get_all_pages
from app.services.transformer import process_and_save
from app.utils.config import settings

app = FastAPI(title="MarginEdge ETL Master")

class SyncResponse(BaseModel):
    status: str
    records: int
    file: Optional[str]

def handle_etl(endpoint: str, params: dict, data_key: str, file_prefix: str) -> SyncResponse:
    try:
        data = get_all_pages(endpoint, params, data_key)
        file = process_and_save(data, file_prefix)
        return SyncResponse(status="Complete", records=len(data), file=file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sync/restaurants", response_model=SyncResponse)
def sync_restaurants() -> SyncResponse:
    """Get all restaurant units."""
    return handle_etl("restaurantUnits", {}, "restaurants", "restaurants")

@app.get("/sync/categories", response_model=SyncResponse)
def sync_categories(restaurantUnitId: str = Query(..., description="Restaurant Unit ID")) -> SyncResponse:
    """Get categories for a specific restaurant."""
    return handle_etl("categories", {"restaurantUnitId": restaurantUnitId}, "categories", "categories")

@app.get("/sync/orders", response_model=SyncResponse)
def sync_orders(
    restaurantUnitId: str = Query(..., description="Restaurant Unit ID"),
    startDate: str = Query(..., description="Start date (YYYY-MM-DD)"),
    endDate: str = Query(..., description="End date (YYYY-MM-DD)")
) -> SyncResponse:
    """Get orders within a date range."""
    params = {"restaurantUnitId": restaurantUnitId, "startDate": startDate, "endDate": endDate}
    return handle_etl("orders", params, "orders", "orders")

@app.get("/sync/order-details/{orderId}", response_model=SyncResponse)
def sync_order_detail(
    orderId: str, 
    restaurantUnitId: str = Query(..., description="Restaurant Unit ID")
) -> SyncResponse:
    """Get full details for a specific order."""
    endpoint = f"orders/{orderId}"
    try:
        headers = {"X-Api-Key": settings.MARGIN_EDGE_API_KEY, "Accept": "application/json"}
        response = requests.get(f"{settings.BASE_URL}/{endpoint}", headers=headers, params={"restaurantUnitId": restaurantUnitId})
        response.raise_for_status()
        data = [response.json()] 
        file = process_and_save(data, f"order_detail_{orderId}")
        return SyncResponse(status="Complete", records=1, file=file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sync/products", response_model=SyncResponse)
def sync_products(restaurantUnitId: str = Query(..., description="Restaurant Unit ID")) -> SyncResponse:
    """Get all products."""
    return handle_etl("products", {"restaurantUnitId": restaurantUnitId}, "products", "products")

@app.get("/sync/vendors", response_model=SyncResponse)
def sync_vendors(restaurantUnitId: str = Query(..., description="Restaurant Unit ID")) -> SyncResponse:
    """Get all vendors."""
    return handle_etl("vendors", {"restaurantUnitId": restaurantUnitId}, "vendors", "vendors")

@app.get("/sync/vendor-items", response_model=SyncResponse)
def sync_vendor_items(
    restaurantUnitId: str = Query(..., description="Restaurant Unit ID"),
    vendorId: str = Query(..., description="Vendor ID")
) -> SyncResponse:
    """Get items for a specific vendor."""
    endpoint = f"vendors/{vendorId}/vendorItems"
    return handle_etl(endpoint, {"restaurantUnitId": restaurantUnitId}, "vendorItems", "vendor_items")

@app.get("/sync/vendor-item-packaging", response_model=SyncResponse)
def sync_vendor_item_packaging(
    restaurantUnitId: str = Query(..., description="Restaurant Unit ID"),
    vendorId: str = Query(..., description="Vendor ID"),
    vendorItemCode: str = Query(..., description="Vendor Item Code")
) -> SyncResponse:
    """Get packaging options for a specific vendor item."""
    endpoint = f"vendors/{vendorId}/vendorItems/{vendorItemCode}/packaging"
    return handle_etl(endpoint, {"restaurantUnitId": restaurantUnitId}, "packagings", "packaging")

@app.get("/sync/group-categories", response_model=SyncResponse)
def sync_group_categories(
    conceptId: Optional[str] = Query(None, description="Concept ID"),
    companyId: Optional[str] = Query(None, description="Company ID")
) -> SyncResponse:
    """Get restaurant unit group categories."""
    params = {}
    if conceptId: params["conceptId"] = conceptId
    if companyId: params["companyId"] = companyId
    return handle_etl("restaurantUnits/groupCategories", params, "groupCategories", "group_categories")