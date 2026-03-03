# MarginEdge ETL Bridge

## Overview
This app is a production-level ETL orchestrator built with FastAPI. It extracts, transforms, and loads data from CSV files for MarginEdge integration.

## Prerequisites
- Python 3.8+
- pip

## Installation
1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd marginedge-etl-bridge
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
- Create a `.env` file in the project root if needed for environment variables.
- Update `app/main.py` with your admin credentials for API authentication.

## Running the App
Start the FastAPI server using Uvicorn:
```bash
uvicorn app.main:app --reload
```

## API Usage
- Access the Swagger UI for API documentation and testing:
  - [http://localhost:8000/docs](http://localhost:8000/docs)
- Main endpoint:
  - `POST /sync/full` — Run the full ETL process. Requires authentication.

## Authentication
- The API uses HTTP Basic authentication. Default credentials are set in `app/main.py`.

## Data Files
- Place your CSV files in the `data/` directory.

## Logging
- Logs are stored in the `logs/` directory.

## Customization
- Modify services in `app/services/` and utilities in `app/utils/` as needed.

## Troubleshooting
- Ensure all dependencies are installed.
- Check the logs for error details.

## License
MIT