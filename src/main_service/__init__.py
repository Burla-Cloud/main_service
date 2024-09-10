import sys
import os
import json
import traceback
import random
import threading
import asyncio
from uuid import uuid4
from time import time, sleep

import pytz
from google.cloud import firestore, logging
from fastapi.responses import Response, HTMLResponse, RedirectResponse
from fastapi import FastAPI, Request, BackgroundTasks, Depends, HTTPException, status
from fastapi.staticfiles import StaticFiles
from google.cloud.firestore_v1 import FieldFilter

from starlette.middleware.sessions import SessionMiddleware
from starlette.datastructures import UploadFile
from starlette.responses import StreamingResponse

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"


TZ = pytz.timezone("America/New_York")
IN_DEV = os.environ.get("IN_DEV") == "True"
PROJECT_ID = "burla-prod" if os.environ.get("IN_PRODUCTION") == "True" else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"
NAS_BUCKET = "burla-nas-prod" if PROJECT_ID == "burla-prod" else "burla-nas"
JOB_ENV_REPO = f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default"

BURLA_BACKEND_URL = "http://10.0.4.35:5002"
# BURLA_BACKEND_URL = "https://backend.test.burla.dev"
# BURLA_BACKEND_URL = "https://backend.burla.dev"

# reduces number of instances / saves across some requests as opposed to using Depends
GCL_CLIENT = logging.Client().logger("main_service")
DB = firestore.Client(project=PROJECT_ID)
test_db = firestore.Client(project="joe-test-407923")


from main_service.helpers import (
    validate_headers_and_login,
    Logger,
    add_logged_background_task,
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


from main_service.endpoints.jobs import router as jobs_router
from main_service.cluster import Cluster


application = FastAPI(docs_url=None, redoc_url=None)
application.include_router(jobs_router)
application.add_middleware(SessionMiddleware, secret_key=uuid4().hex)
application.mount("/static", StaticFiles(directory="src/main_service/static"), name="static")


@application.get("/")
def health_check():
    return json.dumps({"status": "ok"})

@application.get("/favicon.ico")
async def favicon():
    return RedirectResponse(url="/static/favicon.ico")


@application.post("/restart_cluster")
def restart_cluster(
    background_tasks: BackgroundTasks,
    logger: Logger = Depends(get_logger),
):
    start = time()
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
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

    exception_urls = ["/", "/dashboard", "/cluster", "/monitor", "shutdown"]
    if request.url.path not in exception_urls and not request.url.path.startswith("/static"):  # healthchecks shouldn't need to login or dashboard access
        user_info = validate_headers_and_login(request)
        request.state.user_email = user_info.get("email")

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

    if not IN_DEV:
        msg = f"Received {request.method} at {request.url}"
        add_logged_background_task(response.background, logger, logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_logged_background_task(response.background, logger, logger.log, msg, latency=latency)

    return response

# Event to signal when the /cluster process is finished
cluster_done = threading.Event()

@application.on_event("shutdown")
async def shutdown_event():
    # Ensure any ongoing cluster process is stopped
    cluster_done.set()
    print("Server is shutting down, signaling cluster_done.")

@application.get("/dashboard", response_class=HTMLResponse)
async def root():
    # Serve the dashboard HTML file synchronously
    with open("src/main_service/static/dashboard.html", "r") as file:
        html_content = file.read()
    return HTMLResponse(content=html_content)

# Cluster start/restart endpoint
@application.post("/cluster")
async def cluster_endpoint():
    cluster_done.clear()  # Reset the event when the cluster starts

    try:
        # Randomly decide whether to return success or an error
        sleep(5)
        if random.random() >= 0.80:
            # Simulate various possible API errors
            errors = [
                HTTPException(status_code=400, detail="Bad Request"),
                HTTPException(status_code=401, detail="Unauthorized"),
                HTTPException(status_code=403, detail="Forbidden"),
                HTTPException(status_code=404, detail="Not Found"),
                HTTPException(status_code=500, detail="Internal Server Error"),
                HTTPException(status_code=502, detail="Bad Gateway"),
                HTTPException(status_code=503, detail="Service Unavailable"),
                HTTPException(status_code=504, detail="Gateway Timeout"),
            ]
            raise random.choice(errors)  # Raise one of the errors randomly

        await asyncio.sleep(5)
        # If no error, proceed with updating nodes' status to "BOOTING"
        for i in range(20, 23):
            doc_ref = test_db.collection('nodes').document(f'node_{i}')
            doc_ref.set({'status': "BOOTING"})
            await asyncio.sleep(4)  # Simulate time for booting asynchronously

        await asyncio.sleep(5)  # Simulate more time for booting asynchronously

        # Simulate nodes transitioning to "RUNNING"
        for i in range(20, 23):
            await asyncio.sleep(2)  # Simulate a staggered start for each node asynchronously
            doc_ref = test_db.collection('nodes').document(f'node_{i}')
            doc_ref.set({'status': "RUNNING"})

        return {"message": "Cluster started successfully"}

    finally:
        # Signal that the cluster process has finished, regardless of success or failure
        cluster_done.set()

# Monitoring endpoint (independent of the cluster process)
@application.get("/monitor")
async def monitor_cluster():
    streamed_nodes = {}  # Dictionary to store nodes and their last streamed status

    async def node_stream():
        while True:  # Continuously monitor the database without relying on cluster_done
            # Reference the 'nodes' collection
            nodes_ref = test_db.collection('nodes')

            # Create a FieldFilter for the 'status' field to include 'BOOTING' and 'RUNNING'
            status_filter = FieldFilter("status", "in", ["BOOTING", "RUNNING"])
            query = nodes_ref.where(filter=status_filter)

            # Query the documents in the 'nodes' collection that match the filter
            docs = query.stream()

            current_node_ids = set()  # Track nodes currently booting or running

            # Iterate over each document in the query result
            for doc in docs:
                node_data = doc.to_dict()
                node_id = doc.id
                current_status = node_data['status']
                current_node_ids.add(node_id)

                # Stream only nodes that are booting or running and haven't been streamed before
                if current_status in ["BOOTING", "RUNNING"]:
                    previous_status = streamed_nodes.get(node_id)
                    if previous_status != current_status:
                        yield f"data: node_id: {node_id} ==> status: {current_status}\n\n"
                        streamed_nodes[node_id] = current_status  # Update streamed status

            # Stream nodes that are no longer active
            for node_id in list(streamed_nodes.keys()):
                if node_id not in current_node_ids:
                    yield f"data: node_id: {node_id} removed from stream as it's no longer active.\n\n"
                    del streamed_nodes[node_id]

            # Wait 1.5 seconds before checking the status again
            await asyncio.sleep(1.5)

    # Stream the node statuses as a server-sent event (SSE)
    return StreamingResponse(node_stream(), media_type="text/event-stream")