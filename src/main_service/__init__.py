import sys
import os
import json
import traceback
from uuid import uuid4
from time import time

import pytz
from google.cloud import firestore, logging
from fastapi.responses import Response
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from starlette.middleware.sessions import SessionMiddleware


TZ = pytz.timezone("America/New_York")
IN_DEV = os.environ.get("IN_DEV") == "True"
PROJECT_ID = "burla-prod" if os.environ.get("IN_PRODUCTION") == "True" else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"
NAS_BUCKET = "burla-nas-prod" if PROJECT_ID == "burla-prod" else "burla-nas"
JOB_ENV_REPO = f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default"

# BURLA_BACKEND_URL = "http://10.0.4.35:5002"
BURLA_BACKEND_URL = "https://backend.test.burla.dev"
# BURLA_BACKEND_URL = "https://backend.burla.dev"

# reduces number of instances / saves across some requests as opposed to using Depends
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(project=PROJECT_ID)


from main_service.helpers import (
    validate_headers_and_login,
    Logger,
    add_logged_background_task,
    format_traceback,
)


async def get_request_json(request: Request):
    return await request.json()


def get_logger(request: Request):
    return Logger(request)


def get_user_doc(request: Request):
    return request.state.user_doc


from main_service.endpoints.jobs import router as jobs_router
from main_service.endpoints.nas import router as nas_router
from main_service.cluster import Cluster


application = FastAPI(docs_url=None, redoc_url=None)
application.include_router(jobs_router)
application.include_router(nas_router)
application.add_middleware(SessionMiddleware, secret_key=uuid4().hex)


@application.get("/")
def health_check():
    return json.dumps({"status": "ok"})


@application.post("/restart_cluster")
def restart_cluster(background_tasks: BackgroundTasks, logger=Depends(get_logger)):
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    cluster.restart()
    return "Success"


@application.post("/force_restart_cluster")
def force_restart_cluster(
    background_tasks: BackgroundTasks,
    logger: Logger = Depends(get_logger),
):
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    cluster.restart(force=True)
    return "Success"


@application.middleware("http")
async def login__log_and_time_requests__log_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    start = time()
    request.state.uuid = uuid4().hex

    if request.url.path != "/":  # healthchecks shouldn't need to login
        user_doc = validate_headers_and_login(request)
        request.state.user_doc = user_doc
        request.state.user_name = user_doc.get("name")
        request.state.user_email = user_doc.get("email")

    # If `get_logger` was a dependency this will be the second time a Logger is created.
    # This is fine because creating this object only attaches the `request` to a function.
    logger = Logger(request)

    # Important to note that HTTP exceptions do not raise errors here!
    try:
        response = await call_next(request)
    except Exception as e:
        # create new response object to return gracefully.
        response = Response(status_code=500, content="Internal server error.")
        response.background = BackgroundTasks()

        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        add_logged_background_task(
            response.background, logger, logger.log, str(e), "ERROR", traceback=traceback_str
        )

    response_contains_background_tasks = getattr(response, "background") is not None
    if not response_contains_background_tasks:
        response.background = BackgroundTasks()

    message = f"Received {request.method} at {request.url}"
    # add_logged_background_task(response.background, logger, logger.log, message)

    status = response.status_code
    latency = time() - start
    message = f"{request.method} to {request.url} returned {status} after {latency} seconds."
    # add_logged_background_task(response.background, logger, logger.log, message, latency=latency)
    return response
