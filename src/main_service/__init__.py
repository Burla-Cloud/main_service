import sys
import os
import json
import traceback
from uuid import uuid4
from time import time
from typing import Callable
from requests.exceptions import HTTPError

import pytz
from google.cloud import firestore, logging
from fastapi.responses import Response, HTMLResponse, FileResponse
from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.staticfiles import StaticFiles

from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile


TZ = pytz.timezone("America/New_York")
IN_DEV = os.environ.get("IN_DEV") == "True"
IN_PROD = os.environ.get("IN_PRODUCTION") == "True"
PROJECT_ID = "burla-prod" if IN_PROD else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if IN_PROD else "burla-jobs"
JOB_ENV_REPO = f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default"

# gRPC streams will throw some unblockable annoying warnings
os.environ["GRPC_VERBOSITY"] = "ERROR"

# if IN_DEV:
# BURLA_BACKEND_URL = "http://10.0.4.35:5002"
# BURLA_BACKEND_URL = "https://backend.test.burla.dev"
# else:
BURLA_BACKEND_URL = "https://backend.burla.dev"

# reduces number of instances / saves across some requests as opposed to using Depends
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(project=PROJECT_ID)


from main_service.helpers import (
    validate_headers_and_login,
    Logger,
    format_traceback,
)


async def get_request_json(request: Request):
    try:
        return await request.json()
    except:
        form_data = await request.form()
        return json.loads(form_data["request_json"])


async def get_request_files(request: Request):
    """
    If request is multipart/form data load all files and returns as dict of {filename: bytes}
    """
    form_data = await request.form()
    files = {}
    for key, value in form_data.items():
        if isinstance(value, UploadFile):
            files.update({key: await value.read()})

    if files:
        return files


def get_logger(request: Request):
    return Logger(request)


def get_user_email(request: Request):
    return request.state.user_email


def get_add_background_task_function(
    background_tasks: BackgroundTasks, logger: Logger = Depends(get_logger)
):
    def add_logged_background_task(func: Callable, *a, **kw):
        tb_details = traceback.format_list(traceback.extract_stack()[:-1])
        parent_traceback = "Traceback (most recent call last):\n" + format_traceback(tb_details)

        def func_logged(*a, **kw):
            try:
                return func(*a, **kw)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                local_traceback_no_title = "\n".join(format_traceback(tb_details).split("\n")[1:])
                traceback_str = parent_traceback + local_traceback_no_title
                logger.log(message=str(e), severity="ERROR", traceback=traceback_str)

        background_tasks.add_task(func_logged, *a, **kw)

    return add_logged_background_task


from main_service.endpoints.jobs import router as jobs_router
from main_service.cluster import Cluster


application = FastAPI(docs_url=None, redoc_url=None)
application.include_router(jobs_router)
application.add_middleware(SessionMiddleware, secret_key=uuid4().hex)
application.mount("/static", StaticFiles(directory="src/main_service/static"), name="static")


@application.get("/")
def health_check():
    return json.dumps({"status": "ok"})


@application.post("/restart_cluster")
def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()
    cluster = Cluster.from_database(db=DB, logger=logger, add_background_task=add_background_task)
    cluster.restart(force=True)
    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")
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

    # don't authenticate requests to these paths
    public_paths = ["/", "/dashboard", "/restart_cluster", "/favicon.ico"]
    requesting_static_file = request.url.path.startswith("/static")
    request_requires_auth = (request.url.path not in public_paths) and (not requesting_static_file)

    if request_requires_auth:
        try:
            user_info = validate_headers_and_login(request)
            request.state.user_email = user_info.get("email")
        except HTTPError as e:
            if "401" in str(e):
                return Response(status_code=401, content="Unauthorized.")
            else:
                raise e

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
        add_background_task = get_add_background_task_function(response.background, logger=logger)

        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        add_background_task(logger.log, str(e), "ERROR", traceback=traceback_str)

    response_contains_background_tasks = getattr(response, "background") is not None
    if not response_contains_background_tasks:
        response.background = BackgroundTasks()
    add_background_task = get_add_background_task_function(response.background, logger=logger)

    if not IN_DEV:
        msg = f"Received {request.method} at {request.url}"
        add_background_task(logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_background_task(response.background, logger, logger.log, msg, latency=latency)

    return response


@application.get("/dashboard")
async def dashboard():
    return FileResponse("src/main_service/static/dashboard.html")
