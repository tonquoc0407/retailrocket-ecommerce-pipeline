import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from prometheus_fastapi_instrumentator import Instrumentator

from api.db import init_pool, close_pool, get_cursor
from api.logging_conf import configure_logging
from api.routers import recommend, funnel, abandon
from api.routers.abandon import load_model
from api.schemas import PipelineRun

log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    init_pool()
    load_model()
    yield
    close_pool()


app = FastAPI(title="RetailRocket Intelligence API", lifespan=lifespan)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    log.info("request", extra={
        "method": request.method,
        "path": request.url.path,
        "status": response.status_code,
        "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
    })
    return response


app.include_router(recommend.router)
app.include_router(funnel.router)
app.include_router(abandon.router)


@app.get("/pipeline-health", response_model=list[PipelineRun])
def pipeline_health():
    # latest run per task, newest first
    with get_cursor() as cur:
        cur.execute("""
            select distinct on (task_name)
                   task_name, status, rows_processed, duration_seconds,
                   started_at, error_message
            from pipeline_runs
            order by task_name, started_at desc
        """)
        return [PipelineRun(**r) for r in cur.fetchall()]


# /metrics for Prometheus (request latency, count, error rate)
Instrumentator().instrument(app).expose(app)
