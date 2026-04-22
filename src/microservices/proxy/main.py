import os
import random
import logging
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from urllib.parse import urljoin

app = FastAPI()
httpx_client = httpx.AsyncClient()
logging.basicConfig(level=logging.INFO)

async def httpx_request_to_target(request: Request, target_url: str):
    url = urljoin(target_url, request.url.path)
    body = await request.body()
    try:
        response = await httpx_client.request(
            method=request.method,
            url=url,
            params=request.query_params,
            content=body,
            timeout=15
        )
        if response.headers.get("content-type", "").startswith("application/json"):
            return JSONResponse(status_code=response.status_code, content=response.json())
        else:
            return PlainTextResponse(status_code=response.status_code, content=response.content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"router exception: {str(e)}")

# Определяем префиксы для миграции
route_env_prefix = {
    "movies": "MOVIES",
    "events": "EVENTS",
    "users": "USERS",
}

@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def proxy(request: Request, path: str):
    monolith_url = os.getenv('MONOLITH_URL', 'http://monolith:8080').rstrip('/')
    
    if path not in route_env_prefix:
        # Неизвестный путь — всегда в монолит
        return await httpx_request_to_target(request, monolith_url)

    # Для известных префиксов — применяем миграцию
    gradual_migration = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
    env_prefix = route_env_prefix[path]
    service_url = os.getenv(f"{env_prefix}_SERVICE_URL")

    migration_percentage = os.getenv(f"{env_prefix}_MIGRATION_PERCENT", "0")
    if not migration_percentage.isdigit():
        migration_percentage = "0"
    migration_percentage = int(migration_percentage)

    select_upstream = monolith_url  # по умолчанию — монолит

    if gradual_migration:
        if random.randint(1, 100) <= migration_percentage:
            select_upstream = service_url  # микросервис

    # Если путь health-check и выбран монолит — используем /health
    if path.endswith("health") and select_upstream == monolith_url:
        request.scope["path"] = "/health"

    return await httpx_request_to_target(request, select_upstream)

@app.api_route("/health", methods=["GET"])
def health():
    return JSONResponse(status_code=200, content={"status": True})