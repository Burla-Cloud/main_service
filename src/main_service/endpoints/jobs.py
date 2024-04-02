import pickle
from time import time
from uuid import uuid4

from fastapi import APIRouter, Path, Depends, HTTPException, BackgroundTasks
from google.cloud.firestore import FieldFilter, SERVER_TIMESTAMP
from google.cloud.storage import Client, Blob


from main_service import JOBS_BUCKET, JOB_ENV_REPO, DB, get_request_json, get_user_doc, get_logger
from main_service.cluster import Cluster
from main_service.env_builder import start_building_environment
from main_service.helpers import (
    validate_create_job_request,
    validate_job_id,
    create_signed_gcs_urls,
    Logger,
)

router = APIRouter()
LOGSTREAM_BUFFER_SEC = 6


@router.post("/v1/jobs/")
def create_job(
    request_json: dict = Depends(get_request_json),
    user_doc: dict = Depends(get_user_doc),
    logger: Logger = Depends(get_logger),
):
    logger.log(f"created job with {int(request_json['n_inputs'])} input(s)", tell_slack=True)
    validate_create_job_request(request_json)

    image = request_json["image"]
    if (image is None) and not request_json["gpu"]:
        image = f"{JOB_ENV_REPO}/image-nogpu:latest"
    elif (image is None) and request_json["gpu"]:
        image = f"{JOB_ENV_REPO}/image-gpu:latest"

    job_id = str(uuid4())
    n_inputs = int(request_json["n_inputs"])
    input_uris = [f"gs://{JOBS_BUCKET}/{job_id}/inputs/{i}.pkl" for i in range(n_inputs)]
    input_urls = [uri_to_url["url"] for uri_to_url in create_signed_gcs_urls(input_uris)]
    function_uri = f"gs://{JOBS_BUCKET}/{int(time())}/{job_id}/function.pkl"
    function_url = create_signed_gcs_urls([function_uri])[0]["url"]

    job_ref = DB.collection("jobs").document(job_id)
    job_ref.set(
        {
            "function_uri": function_uri,
            "n_sub_jobs": n_inputs,
            "func_cpu": request_json["func_cpu"],
            "func_ram": request_json["func_ram"],
            "gpu": request_json["gpu"],
            "python_version": request_json["python_version"],
            "parallelism": request_json["parallelism"],
            "user": user_doc.get("email"),
            "started_at": SERVER_TIMESTAMP,
            "burla_client_version": request_json["burla_version"],
            "env": {"is_copied_from_client": False, "image": image},
        }
    )

    subjob_collection = job_ref.collection("sub_jobs")
    for subjob_id in range(n_inputs):
        subjob_collection.document(str(subjob_id)).set({"claimed": False})

    if request_json.get("packages"):
        start_building_environment(request_json["packages"], job_ref, job_id, image=image)

    # store the urls in a blob then download blob on client because max cloud run response is 32MB
    input_urls_uri = f"gs://{JOBS_BUCKET}/{job_id}/inputs/urls.json"
    blob = Blob.from_string(input_urls_uri, Client())
    blob.upload_from_string(pickle.dumps(input_urls), content_type="application/octet-stream")
    input_urls_url = create_signed_gcs_urls([input_urls_uri], method="GET")[0]["url"]

    return {"job_id": job_id, "function_url": function_url, "input_urls_url": input_urls_url}


@router.post("/v1/jobs/{job_id}")
def start_job(
    background_tasks: BackgroundTasks,
    job_id: str = Path(...),
    logger: Logger = Depends(get_logger),
):
    validate_job_id(job_id, DB, logger)
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    cluster.execute(job_id=job_id)
    return {"job_id": job_id}


@router.get("/v1/jobs/{job_id}/{epoch}")
def get_job_info(
    background_tasks: BackgroundTasks,
    job_id: str = Path(...),
    epoch: str = Path(...),
    logger: Logger = Depends(get_logger),
):
    job_ref = DB.collection("jobs").document(job_id)
    sub_jobs_ref = job_ref.collection("sub_jobs")
    job = job_ref.get().to_dict()

    doc_ref_udf_started = sub_jobs_ref.where(filter=FieldFilter("udf_started", "==", True))
    udf_started = bool(next(doc_ref_udf_started.limit(1).stream(), False))

    # collect logs since last request for job info
    # Explaination why user will always recieve every log: https://pastebin.com/WXnqjGH6
    try:
        filter_1 = FieldFilter("epoch", ">", int(epoch))
        filter_2 = FieldFilter("epoch", "<", int(time()) - 1)  # <- important ^^
        log_docs = job_ref.collection("logs").where(filter=filter_1).where(filter=filter_2).stream()
        logs = [(log.get("epoch"), log.get("text")) for log in log_docs]
    except Exception as e:
        logs = [(int(time()), "ERROR: Unable to stream logs, volume is too high.")]
        logger.log_exception(e, tell_slack=False, job_id=job_id)

    # udf_error ?
    sub_jobs_with_udf_error = sub_jobs_ref.where(filter=FieldFilter("udf_error", ">=", ""))
    doc_with_udf_error = next(sub_jobs_with_udf_error.limit(1).stream(), None)
    udf_error = doc_with_udf_error.to_dict()["udf_error"] if doc_with_udf_error else None
    if udf_error:
        logger.log(f"received an error (their fault not ours) for job `{job_id}`", tell_slack=True)
        return {"udf_started": udf_started, "logs": logs, "udf_error": udf_error}

    # install_error ?
    install_error = job.get("env", {}).get("install_error")
    if install_error:
        logger.log(f"received install error for job `{job_id}`", tell_slack=True)
        return {"udf_started": udf_started, "logs": logs, "install_error": install_error}

    # server error ?
    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    server_error = cluster.status(job_id=job_id) == "FAILED"
    if server_error:
        logger.log(f"received an error (OUR FAULT! not theirs) for job `{job_id}`", tell_slack=True)
        raise HTTPException(500)

    # job_succeeded ?
    sub_jobs = list(sub_jobs_ref.where(filter=FieldFilter("done", "==", True)).stream())
    job_succeeded = job["n_sub_jobs"] == len(sub_jobs)
    if job_succeeded:
        output_uris = [f"gs://{JOBS_BUCKET}/{job_id}/outputs/{i}.pkl" for i in range(len(sub_jobs))]
        uris_to_urls = create_signed_gcs_urls(output_uris, method="GET")
        output_urls = [uri_to_url["url"] for uri_to_url in uris_to_urls]
        logger.log(f"received all return values for job `{job_id}`", tell_slack=True)
        return {"udf_started": udf_started, "logs": logs, "output_urls": output_urls}

    # (Job still running)
    return {"udf_started": udf_started, "logs": logs}
