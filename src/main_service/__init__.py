import os
import json
from uuid import uuid4
from time import time

import pytz
from google.cloud import firestore, logging
from fastapi import FastAPI, Depends, Request, BackgroundTasks
from starlette.middleware.sessions import SessionMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

TZ = pytz.timezone("America/New_York")
PROJECT_ID = "burla-prod" if os.environ.get("IN_PRODUCTION") == "True" else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"
NAS_BUCKET = "burla-nas-prod" if PROJECT_ID == "burla-prod" else "burla-nas"
JOB_ENV_REPO = f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-environments"

JAKE_SLACK_ID = "<@U04C58VF24C>"
MUTED_USERS = ["joe@burla.dev", "jake@burla.dev", "jack@burla.dev"]

BURLA_BACKEND_URL = "https://backend.test.burla.dev"
# BURLA_BACKEND_URL = "https://backend.burla.dev"

# reduces number of instances / saves across some requests as opposed to using Depends
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(project=PROJECT_ID)

from main_service.helpers import validate_headers_and_login, Logger


async def get_request_json(request: Request):
    return await request.json()


def get_user_doc(request: Request):
    return validate_headers_and_login(request)


def get_logger(request: Request, user_doc: dict = Depends(get_user_doc)):
    can_tell_slack = (PROJECT_ID == "burla-prod") and (user_doc.get("email") not in MUTED_USERS)
    return Logger(
        request=request, user_doc=user_doc, gcl_client=GCL_CLIENT, can_tell_slack=can_tell_slack
    )


def log_uncaught_exceptions(logger: Logger = Depends(get_logger)):
    """fastapi dependencies cannot be used in `@app.exception_handler`s, this is a workaround."""
    try:
        yield
    except Exception as e:
        if not isinstance(e, StarletteHTTPException):
            logger.log_exception(e, tell_slack=True)
        raise e


from main_service.endpoints.jobs import router as jobs_router
from main_service.endpoints.nas import router as nas_router
from main_service.cluster import Cluster


application = FastAPI(
    docs_url=None, redoc_url=None, dependencies=[Depends(log_uncaught_exceptions)]
)
application.include_router(jobs_router)
application.include_router(nas_router)
application.add_middleware(SessionMiddleware, secret_key=uuid4().hex)


@application.get("/")
def health_check():
    return json.dumps({"status": "ok"})


@application.post("/restart_cluster")
def restart_cluster(background_tasks: BackgroundTasks, logger: Logger = Depends(get_logger)):
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    cluster.restart()
    return "Success"


@application.post("/force_restart_cluster")
def force_restart_cluster(background_tasks: BackgroundTasks, logger: Logger = Depends(get_logger)):
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    cluster.restart(force=True)
    return "Success"


@application.middleware("http")
async def log_and_time_requests(request: Request, call_next):
    start = time()
    request.state.uuid = uuid4().hex
    logger = Logger(request=request, gcl_client=GCL_CLIENT, can_tell_slack=False)
    logger.log(f"Received {request.method} at {request.url}", severity="DEBUG")

    response = await call_next(request)

    status = response.status_code
    msg = f"{request.method} to {request.url} returned {status} after {time()-start} seconds."

    default = lambda o: "<not serializable>"
    loggable_response = json.dumps(vars(response), skipkeys=True, default=default)
    logger.log(msg, response=loggable_response, severity="DEBUG")
    return response
