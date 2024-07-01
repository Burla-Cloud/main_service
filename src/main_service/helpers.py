import sys
import json
import requests
import traceback
from itertools import groupby
from typing import Literal, Callable
from time import sleep
from concurrent.futures import ThreadPoolExecutor

import google
from fastapi import Request, HTTPException, BackgroundTasks
from google.cloud import firestore
from google.oauth2.id_token import fetch_id_token
from google.cloud.secretmanager import SecretManagerServiceClient

from main_service import PROJECT_ID, BURLA_BACKEND_URL, IN_DEV, GCL_CLIENT


def get_secret(secret_name: str):
    client = SecretManagerServiceClient()
    secret_path = client.secret_version_path(PROJECT_ID, secret_name, "latest")
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


def format_traceback(traceback_details: list):
    details = ["  ... (detail hidden)\n" if "/pypoetry/" in d else d for d in traceback_details]
    details = [key for key, _ in groupby(details)]  # <- remove consecutive duplicates
    return "".join(details).split("another exception occurred:")[-1]


class Logger:

    def __init__(self, request: Request):
        self.loggable_request = self.__loggable_request(request)

    def __make_serializeable(self, obj):
        """
        Recursively traverses a nested dict swapping any:
        - tuple -> list
        - !dict or !list or !str -> str
        """
        if isinstance(obj, tuple) or isinstance(obj, list):
            return [self.__make_serializeable(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self.__make_serializeable(value) for key, value in obj.items()}
        elif not (isinstance(obj, dict) or isinstance(obj, list) or isinstance(obj, str)):
            return str(obj)
        else:
            return obj

    def __loggable_request(self, request: Request):
        keys = ["asgi", "client", "headers", "http_version", "method", "path", "path_params"]
        keys.extend(["query_string", "raw_path", "root_path", "scheme", "server", "state", "type"])
        scope = {key: request.scope.get(key) for key in keys}
        request_dict = {
            "scope": scope,
            "url": str(request.url),
            "base_url": str(request.base_url),
            "headers": request.headers,
            "query_params": request.query_params,
            "path_params": request.path_params,
            "cookies": request.cookies,
            "client": request.client,
            "method": request.method,
        }
        # google cloud logging won't log tuples or bytes objects.
        return self.__make_serializeable(request_dict)

    def log(self, message: str, severity="INFO", **kw):
        if IN_DEV and "traceback" in kw.keys():
            print(f"\nERROR: {message.strip()}\n{kw['traceback'].strip()}\n", file=sys.stderr)
        elif IN_DEV:
            print(message)
        else:
            struct = dict(message=message, request=self.loggable_request, **kw)
            GCL_CLIENT.log_struct(struct, severity=severity)


def add_logged_background_task(
    background_tasks: BackgroundTasks, logger: Logger, func: Callable, *a, **kw
):
    """
    Errors thrown in background tasks are completely hidden and ignored by default.
    - BackgroundTasks class cannot be reliably monkeypatched
    - BackgroundTasks cannot be reliably modified in middleware
    - BackgroundTasks cannot be returned by dependencies (`fastapi.Depends`)
    Hopefully I remember to use this function everytime I would normally call `.add_task` 😀🔫
    """
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


def validate_create_job_request(request_json: dict):
    if request_json["python_version"] not in ["3.8", "3.9", "3.10", "3.11", "3.12"]:
        raise HTTPException(400, detail="invalid python version, } [3.8, 3.9, 3.10, 3.11, 3.12]")
    elif (request_json["func_cpu"] > 96) or (request_json["func_cpu"] < 1):
        raise HTTPException(400, detail="invalid func_cpu, must be in [1.. 96]")
    elif (request_json["func_ram"] > 624) or (request_json["func_ram"] < 1):
        raise HTTPException(400, detail="invalid func_ram, must be in [1.. 624]")
    # elif (request_json["func_gpu"] > 4) or (request_json["func_ram"] < 1):
    #     abort(400, "invalid func_gpu, must be in [1.. 4]")


def validate_headers_and_login(request: Request):

    headers = {"authorization": request.headers.get("Authorization")}
    if request.headers.get("Email"):
        headers["Email"] = request.headers.get("Email")

    response = requests.get(f"{BURLA_BACKEND_URL}/v1/user/info", headers=headers)
    response.raise_for_status()
    return response.json()


def validate_job_id(
    job_id: str,
    db: firestore.Client,
    logger: Logger,
):
    job_ref = db.collection("jobs").document(job_id)
    if not job_ref.get().exists:
        logger.log("attempted to access job that does not exist?")
        raise HTTPException(404)


def get_oauth2_id_token(audience):
    credentials, _ = google.auth.default()
    session = google.auth.transport.requests.AuthorizedSession(credentials)
    request = google.auth.transport.requests.Request(session)
    credentials.refresh(request)
    if hasattr(credentials, "id_token"):
        return credentials.id_token
    return fetch_id_token(request, audience)


def create_signed_gcs_urls(
    uris: list[str],
    duration_min: int = 30,
    method: Literal["PUT", "GET"] = "PUT",
    content_type: str = "application/octet-stream",
):
    """
    Why:
    Blob.generate_signed_url can be really slow (~0.25s). Sometimes we need to generate millions
    of these. 0.25s * 10^9 is a lot of time to wait.
    What:
    There are two cloud functions used here setup like a map-reduce.
    CF1: `create_signed_gcs_urls`: exists because I cant get more than 30 concurrent threads
    going for some reason when running inside this wsgi/gunicorn flask server. This first CF just
    calls the second with a lot (thousands) of concurrent http calls.
    CF2: `create_signed_gcs_url`: simply creates a signed gcs url from one gcs uri.
    """
    function_url = f"https://us-central1-{PROJECT_ID}.cloudfunctions.net/create_signed_gcs_urls"
    svc_account_credentials = json.loads(get_secret("gcs_url_creator_svc_account"))
    id_token = get_oauth2_id_token(audience=function_url)
    auth_headers = {"Authorization": f"Bearer {id_token}", "Content-Type": "application/json"}

    uris_per_batch = 1000  # CF takes about 3s / 1000
    uri_batches = [uris[i : i + uris_per_batch] for i in range(0, len(uris), uris_per_batch)]

    def _create_signed_gcs_urls(_uris, retries=5, backoff=0.5):
        for i in range(retries):
            try:
                payload = {
                    "uris": _uris,
                    "duration_min": duration_min,
                    "svc_creds": svc_account_credentials,
                    "method": method,
                    "content_type": content_type,
                }
                response = requests.post(
                    function_url, headers=auth_headers, data=json.dumps(payload)
                )
                response.raise_for_status()
                return response.json()["uris_to_urls"]
            except:
                if i < retries - 1:
                    sleep(backoff * 2**i)
                else:
                    raise

    with ThreadPoolExecutor() as executor:
        uris_to_urls = list(executor.map(_create_signed_gcs_urls, uri_batches))
    return [uri_to_url for uris_to_urls_sub in uris_to_urls for uri_to_url in uris_to_urls_sub]
