import sys
import json
import requests
import traceback
from time import time
from uuid import uuid4
from typing import Optional

import cloudpickle
from fastapi import APIRouter, Path, Depends, HTTPException, BackgroundTasks
from google.cloud.firestore import FieldFilter, SERVER_TIMESTAMP, Increment
from google.cloud.firestore_v1 import aggregation


from main_service import (
    JOBS_BUCKET,
    JOB_ENV_REPO,
    DB,
    get_user_email,
    get_logger,
    get_request_json,
    get_request_files,
)
from main_service.cluster import Cluster, parallelism_capacity, reboot_nodes_with_job
from main_service.env_builder import start_building_environment
from main_service.helpers import (
    validate_create_job_request,
    Logger,
    add_logged_background_task,
)

router = APIRouter()
LOGSTREAM_BUFFER_SEC = 6


@router.post("/v1/jobs/")
def create_job(
    background_tasks: BackgroundTasks,
    request_json: dict = Depends(get_request_json),
    user_email: dict = Depends(get_user_email),
    logger: Logger = Depends(get_logger),
    request_files: Optional[dict] = Depends(get_request_files),
):
    create_request_receive = time()
    logger.log(f"received request to create job.")
    validate_create_job_request(request_json)

    job_id = str(uuid4())
    job_ref = DB.collection("jobs").document(job_id)
    n_inputs = int(request_json["n_inputs"])
    inputs_in_gcs = not bool(request_files)
    image = request_json["image"]
    if (image is None) and not request_json["gpu"]:
        image = f"{JOB_ENV_REPO}/image-nogpu:latest"
    elif (image is None) and request_json["gpu"]:
        image = f"{JOB_ENV_REPO}/image-gpu:latest"

    job_ref.set(
        {
            "n_sub_jobs": n_inputs,
            "inputs_in_gcs": inputs_in_gcs,
            "func_cpu": request_json["func_cpu"],
            "func_ram": request_json["func_ram"],
            "gpu": request_json["gpu"],
            "target_parallelism": request_json["parallelism"],
            "current_parallelism": 0,
            "user": user_email,
            "started_at": SERVER_TIMESTAMP,
            "started_at_ts": time(),
            "ended_at": None,
            "burla_client_version": request_json["burla_version"],
            "env": {
                "is_copied_from_client": False,
                "image": image,
                "python_version": request_json["python_version"],
            },
            "benchmark": {
                "rpm_call_time": request_json["rpm_call_time"],
                "create_job_request_received": create_request_receive,
            },
        }
    )
    subjob_collection = job_ref.collection("sub_jobs")

    if request_json.get("packages"):
        start_building_environment(request_json["packages"], job_ref, job_id, image=image)
    if inputs_in_gcs:
        for id in range(n_inputs):
            subjob_collection.document(str(id)).set({"claimed": False})
    else:
        for id, input_pkl in enumerate(cloudpickle.loads(request_files["inputs_pkl"])):
            subjob_collection.document(str(id)).set({"claimed": False, "input_pkl": input_pkl})

    ready_nodes_filter = FieldFilter("status", "==", "READY")
    ready_nodes = DB.collection("nodes").where(filter=ready_nodes_filter).stream()

    current_parallelism = 0
    for node_ref in ready_nodes:
        node_doc = node_ref.to_dict()

        current_parallelism = 0
        max_parallelism = parallelism_capacity(
            node_doc["machine_type"],
            request_json["func_cpu"],
            request_json["func_ram"],
            request_json["gpu"],
        )
        compatible_with_job = max_parallelism > 0

        if compatible_with_job:
            parallelism = min([request_json["parallelism"] - current_parallelism, max_parallelism])
            payload = {"parallelism": parallelism}
            if inputs_in_gcs:
                response = requests.post(f"{node_doc['host']}/jobs/{job_id}", json=payload)
            else:
                files = dict(function_pkl=request_files["function_pkl"])
                data = dict(request_json=json.dumps(payload))
                url = f"{node_doc['host']}/jobs/{job_id}"
                response = requests.post(url, files=files, data=data)

            try:
                response.raise_for_status()
            except Exception as e:
                # this error means the node is already working on a job.
                if "409" not in str(e):
                    raise e

            # I've chosed not to schedule new nodes starting here because the job might be really
            # short meaning it's not worth starting a new node.
            logger.log(f"Assigned node {node_doc['instance_name']} to job {job_id}.")
            current_parallelism += parallelism

            update_node = node_ref.reference.update
            new_node_info = {"status": "RUNNING", "current_job": job_id}
            add_logged_background_task(background_tasks, logger, update_node, new_node_info)

        if current_parallelism >= request_json["parallelism"]:
            break

    new_job_info = {"current_parallelism": Increment(current_parallelism)}
    add_logged_background_task(background_tasks, logger, job_ref.update, new_job_info)

    # TODO: if parallelism is not >= request_json["parallelism"] then start additional nodes!

    if current_parallelism == 0:
        raise Exception("no ready nodes available.")

    logger.log(f"done starting job.")

    # TODO: when `not inputs_in_gcs`:
    # I need to schedule a background task here that will upload them to gcs!
    # There are situations (reconcile, reassign) where a job needs to start executing,
    # and isn't connected to this chain of requests containing the inputs/function.

    job_ref.update({"benchmark.create_job_response": time()})
    return {"job_id": job_id}


@router.get("/v1/jobs/{job_id}/{epoch}")
def get_job_info(
    background_tasks: BackgroundTasks,
    job_id: str = Path(...),
    epoch: str = Path(...),
    logger: Logger = Depends(get_logger),
):
    first_request_receive_time = time()

    job_ref = DB.collection("jobs").document(job_id)
    job = job_ref.get().to_dict()
    sub_jobs_ref = job_ref.collection("sub_jobs")

    # wait up to 5 sec for job to be done, enables endpoint to reply faster if job is done.
    JOB_POLL_DURATION_SEC = 5
    start = time()
    done_watching = False
    job_is_done = False
    while not (job_is_done or done_watching):
        sub_jobs_ref = job_ref.collection("sub_jobs")
        query = sub_jobs_ref.where(filter=FieldFilter("done", "==", True))
        num_done_subjobs = aggregation.AggregationQuery(query).count(alias="all").get()[0][0].value
        job_is_done = num_done_subjobs == job["n_sub_jobs"]
        done_watching = (time() - start) > JOB_POLL_DURATION_SEC

    # should be recorded asap after success detected (not after logs collected).
    if job_is_done:
        job_ref.update({"benchmark.job_success_detected": time()})

    # collect logs since last request for job info
    # Explaination why user will always recieve every log: https://pastebin.com/WXnqjGH6
    try:
        filter_1 = FieldFilter("epoch", ">", int(epoch))
        filter_2 = FieldFilter("epoch", "<", int(time()) - 1)  # <- important ^^
        log_docs = job_ref.collection("logs").where(filter=filter_1).where(filter=filter_2).stream()
        logs = [(log.get("epoch"), log.get("text")) for log in log_docs]
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = "".join(traceback_details).split("another exception occurred:")[-1]
        msg = f"IGNORING LOG ERROR: {e}"
        logger.log(msg, severity="ERROR", traceback=traceback_str, job_id=job_id)
        logs = [(int(time()), "ERROR: Unable to stream logs, volume is too high.")]

    if job_is_done:
        # schedule reboot of any nodes this job ran on (time sensitive)
        add_logged_background_task(background_tasks, logger, reboot_nodes_with_job, DB, job_id)
        # schedule reconcile
        # reconcile = lambda: Cluster.from_database(DB, logger, background_tasks).reconcile()
        # add_logged_background_task(background_tasks, logger, reconcile)
        return {"udf_started": True, "logs": logs, "done": True}

    cluster = Cluster.from_database(db=DB, logger=logger, background_tasks=background_tasks)
    job_status = cluster.status(job_id=job_id)

    # record time of first call to this function for benchmarking purposes
    is_first_request = not job["benchmark"].get("first_job_info_request_receive")
    if is_first_request:
        job_ref.update({"benchmark.first_job_info_request_receive": first_request_receive_time})

    # udf_error ?  (error in user's code)
    sub_jobs_with_udf_error = sub_jobs_ref.where(filter=FieldFilter("udf_error", ">=", ""))
    doc_with_udf_error = next(sub_jobs_with_udf_error.limit(1).stream(), None)
    udf_error = doc_with_udf_error.to_dict()["udf_error"] if doc_with_udf_error else None
    if udf_error:
        logger.log(f"received an error in UDF for job `{job_id}`")
        return {"udf_started": True, "logs": logs, "udf_error": udf_error}

    # install_error ?  (error in user's environment)
    install_error = job.get("env", {}).get("install_error")
    if install_error:
        logger.log(f"received install error for job `{job_id}`")
        return {"udf_started": True, "logs": logs, "install_error": install_error}

    # server error ?  (error in burla's code)
    if job_status == "FAILED":
        logger.log(f"Received a server error (not a UDF error) for job `{job_id}`!")
        raise HTTPException(500)

    # (Job still running)
    return {"udf_started": True, "logs": logs}


@router.post("/v1/jobs/{job_id}/done")
def report_job_done(job_id: str = Path(...), request_json: dict = Depends(get_request_json)):
    job_ref = DB.collection("jobs").document(job_id)
    job_ref.update({"benchmark.job_done_ts": request_json["job_done_ts"]})

    # TEMP: print benchmark results:
    benchmark = job_ref.get().to_dict()["benchmark"]
    rpm_call_time = benchmark.pop("rpm_call_time")
    benchmark_sorted = dict(sorted(benchmark.items(), key=lambda item: item[1]))
    max_key_length = max(len(str(item[0])) for item in benchmark_sorted.items())

    padding = " " * (max_key_length - len("RPM called at"))
    print(f"\nRPM called at:{padding}\tT+0.0s")

    last_ts = rpm_call_time
    for checkpoint, ts in benchmark_sorted.items():
        padding = " " * (max_key_length - len(checkpoint))
        time_since_start = round(ts - rpm_call_time, 1)
        time_since_last = round(ts - last_ts, 1)
        print(f"{checkpoint}:{padding}\tT+{time_since_start}s\tL+{time_since_last}s")
        last_ts = ts

    padding = " " * (max_key_length - len("Total e2e runtime"))
    print(f"Total e2e runtime:{padding}\t{benchmark['job_done_ts']-rpm_call_time}s\n")

    return {}
