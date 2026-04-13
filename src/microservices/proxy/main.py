import os
import random
import logging
from typing import Dict, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
import httpx
from starlette.background import BackgroundTask

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Environment variables ---
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081").rstrip("/")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))

def get_target_url() -> str:
    if GRADUAL_MIGRATION:
        if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
            return MOVIES_SERVICE_URL
    return MONOLITH_URL

@app.api_route("/api/movies/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def proxy_movies(full_path: str, request: Request):
    original_path = f"/api/movies/{full_path}"
    target_base = get_target_url()
    url = f"{target_base}{original_path}"

    query_string = request.url.query
    if query_string:
        url += f"?{query_string}"

    logger.info(f"[PROXY] Routing {request.method} {original_path} → {target_base}")

    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)
    headers.pop("transfer-encoding", None)

    async with httpx.AsyncClient() as client:
        try:
            body = await request.body() if request.method in ("POST", "PUT", "PATCH") else b""
            resp = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body,
                follow_redirects=True
            )

            # Удаляем конфликтующие заголовки перед отправкой клиенту
            out_headers = dict(resp.headers)
            out_headers.pop("content-length", None)
            out_headers.pop("transfer-encoding", None)
            out_headers.pop("connection", None)  # безопасно удалить

            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=out_headers,
                media_type=resp.headers.get("content-type")
            )
        except httpx.RequestError as e:
            logger.error(f"Upstream request failed: {e}")
            raise HTTPException(status_code=502, detail="Upstream service unavailable")

# Все остальные запросы — в монолит
@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def proxy_all(full_path: str, request: Request):
    original_path = f"/{full_path}"
    url = f"{MONOLITH_URL}{original_path}"

    query_string = request.url.query
    if query_string:
        url += f"?{query_string}"

    logger.info(f"[PROXY] Routing {request.method} {original_path} → monolith")

    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)
    headers.pop("transfer-encoding", None)

    async with httpx.AsyncClient() as client:
        try:
            body = await request.body() if request.method in ("POST", "PUT", "PATCH") else b""
            resp = await client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body,
                follow_redirects=True
            )

            out_headers = dict(resp.headers)
            out_headers.pop("content-length", None)
            out_headers.pop("transfer-encoding", None)
            out_headers.pop("connection", None)

            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=out_headers,
                media_type=resp.headers.get("content-type")
            )
        except httpx.RequestError as e:
            logger.error(f"Upstream request failed: {e}")
            raise HTTPException(status_code=502, detail="Upstream service unavailable")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    logger.info(f"✅ Proxy service started on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)