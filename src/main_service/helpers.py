import os
import sys
import json
import requests
import traceback
from typing import Literal, Optional
from time import sleep
from concurrent.futures import ThreadPoolExecutor

import slack_sdk
import google
from fastapi import Request, HTTPException
from google.cloud import logging
from google.cloud import firestore
from google.oauth2.id_token import fetch_id_token
from google.cloud.secretmanager import SecretManagerServiceClient

from main_service import PROJECT_ID, JAKE_SLACK_ID, BURLA_BACKEND_URL


def get_secret(secret_name: str):
    client = SecretManagerServiceClient()
    secret_path = client.secret_version_path(PROJECT_ID, secret_name, "latest")
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


class Logger:
    def __init__(
        self,
        request: Request,
        gcl_client: logging.Client,
        can_tell_slack: bool,
        user_doc: Optional[dict] = {},  # <- understand implications !
    ):
        self.request = request
        self.user = {"name": user_doc.get("name"), "email": user_doc.get("email")}
        self.user_prefix = f"{self.user['name']} ({self.user['email']}) " if user_doc else ""
        self.gcl_client = gcl_client
        self.can_tell_slack = can_tell_slack
        if self.can_tell_slack:
            self.slack_token = get_secret("slackbot-token")
        self.in_dev = os.environ.get("IN_DEV") == "True"

    def _loggable_request(self):
        return json.dumps(vars(self.request), skipkeys=True, default=lambda o: "<not serializable>")

    def _send_slack_message(self, message: str):
        try:
            client = slack_sdk.WebClient(token=self.slack_token)
            client.chat_postMessage(channel="user-activity", text=message)
        except Exception as e:
            self.can_tell_slack = False
            self.log_exception(e)
            self.can_tell_slack = True

    def log(
        self,
        msg: str,
        tell_slack: bool = False,
        **kw,
    ):
        request = self._loggable_request()
        message = f"{self.user_prefix}{msg}"
        struct = dict(message=message, user=self.user, request=request, **kw)
        self.gcl_client.log_struct(struct)

        if self.in_dev:
            print(message)

        if tell_slack and self.can_tell_slack:
            self._send_slack_message(message, token=self.slack_token, gcl_client=self.gcl_client)

    def log_exception(self, e: Exception, tell_slack: bool = True, **kw):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = "".join(traceback_details)

        if tell_slack and self.can_tell_slack:
            status_code = getattr(e, "status_code", 500)
            path = self.request.url.path
            job_id = path.split("job_id/")[1].split("/")[0] if "job_id" in path else "???"
            message = f"{self.user_prefix}received error: {status_code} for job `{job_id}`!"
            message += " " + JAKE_SLACK_ID
            self._send_slack_message(message, token=self.slack_token, gcl_client=self.gcl_client)

        struct = {
            "severity": "ERROR",
            "message": str(e),
            "traceback": traceback_str,
            "request": self._loggable_request(),
            "user": self.user,
            **kw,
        }
        self.gcl_client.log_struct(struct)


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
