from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from app.services.orchestrator import run_full_etl
from app.utils.logger import logger

app = FastAPI(
    title="MarginEdge ETL Master",
    version="2.0",
    description="Production Level ETL Orchestrator"
)

@app.post("/sync/full", tags=["Master ETL"])
async def sync_full(
    background_tasks: BackgroundTasks,
    startDate: str = Query(..., description="Start Date (YYYY-MM-DD)"),
    endDate: str = Query(..., description="End Date (YYYY-MM-DD)")
):
    try:
        background_tasks.add_task(run_full_etl, startDate, endDate)
        logger.info("ETL Job Triggered via API")

        return {
            "status": "ETL Job Started",
            "message": "Process running in background. Check logs for progress."
        }
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))