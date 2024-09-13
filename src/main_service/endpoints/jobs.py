import json
import requests
from time import time
from uuid import uuid4
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Path, Depends
from fastapi.responses import Response
from google.cloud.firestore import FieldFilter, SERVER_TIMESTAMP, Increment


from main_service import (
    JOB_ENV_REPO,
    DB,
    get_user_email,
    get_logger,
    get_request_json,
    get_request_files,
    get_add_background_task_function,
)
from main_service.cluster import Cluster, parallelism_capacity, reboot_nodes_with_job, reconcile
from main_service.env_builder import start_building_environment
from main_service.helpers import validate_create_job_request, Logger

router = APIRouter()


@router.post("/v1/jobs/")
def create_job(
    user_email: dict = Depends(get_user_email),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    create_request_receive = time()
    logger.log(f"received request to create job.")
    validate_create_job_request(request_json)

    job_id = str(uuid4())
    job_ref = DB.collection("jobs").document(job_id)
    n_inputs = int(request_json["n_inputs"])
    function_in_gcs = not bool(request_files)
    image = request_json["image"]
    if (image is None) and not request_json["func_gpu"]:
        image = f"{JOB_ENV_REPO}/image-nogpu:latest"
    elif (image is None) and request_json["func_gpu"]:
        image = f"{JOB_ENV_REPO}/image-gpu:latest"

    job_ref.set(
        {
            "n_sub_jobs": n_inputs,
            "inputs_id": request_json["inputs_id"],
            "function_in_gcs": function_in_gcs,
            "func_cpu": request_json["func_cpu"],
            "func_ram": request_json["func_ram"],
            "func_gpu": request_json["func_gpu"],
            "target_parallelism": request_json["parallelism"],
            "current_parallelism": 0,
            "user": user_email,
            "started_at": SERVER_TIMESTAMP,
            "started_at_ts": time(),
            "ended_at": None,
            "udf_errors": [],
            "burla_client_version": request_json["burla_version"],
            "env": {
                "is_copied_from_client": False,
                "image": image,
                "python_version": request_json["python_version"],
            },
            "benchmark": {"create_job_request_received": create_request_receive},
        }
    )

    if request_json.get("packages"):
        start_building_environment(request_json["packages"], job_ref, job_id, image=image)

    # figure out which nodes will work on this job & at what parallelism
    job_ref.update({f"benchmark.node_assignment_begin": time()})
    ready_nodes_filter = FieldFilter("status", "==", "READY")
    ready_nodes = DB.collection("nodes").where(filter=ready_nodes_filter).stream()

    planned_future_job_parallelism = 0
    nodes_to_assign = []
    for node_ref in ready_nodes:
        node_doc = node_ref.to_dict()
        max_node_parallelism = parallelism_capacity(
            node_doc["machine_type"],
            request_json["func_cpu"],
            request_json["func_ram"],
            request_json["func_gpu"],
        )

        if max_node_parallelism > 0:
            parallelism_deficit = request_json["parallelism"] - planned_future_job_parallelism
            node_target_parallelism = min(parallelism_deficit, max_node_parallelism)
            node_doc["target_parallelism"] = node_target_parallelism
            nodes_to_assign.append(node_doc)
            planned_future_job_parallelism += node_target_parallelism
            add_background_task(
                node_ref.reference.update, dict(target_parallelism=node_target_parallelism)
            )

    if request_json["parallelism"] > planned_future_job_parallelism:
        # TODO: start more nodes here to fill the gap
        parallelism_deficit = request_json["parallelism"] - planned_future_job_parallelism
        msg = f"Cluster needs {parallelism_deficit} more cpus, "
        msg += f"continuing with a parallelism of {planned_future_job_parallelism}."
        logger.log(msg, severity="WARNING")

    # concurrently ask them all to start work:
    def assign_node(node_doc: dict):
        """Errors in here are raised correctly!"""
        payload = {"parallelism": node_doc["target_parallelism"]}
        if function_in_gcs:
            response = requests.post(f"{node_doc['host']}/jobs/{job_id}", json=payload)
        else:
            files = dict(function_pkl=request_files["function_pkl"])
            data = dict(request_json=json.dumps(payload))
            url = f"{node_doc['host']}/jobs/{job_id}"
            response = requests.post(url, files=files, data=data)

        error = False
        try:
            response.raise_for_status()
        except Exception as e:
            # Any errors returned here should also be raised inside the node service.
            # Errors here shouldn't kill the job because some workers are often able to start.
            # Nodes returning errors here should be restarted.
            error = True
            msg = f"Node {node_doc['instance_name']} refused job with error: {e}"
            logger.log(msg, severity="WARNING")

        if not error:
            logger.log(f"Assigned node {node_doc['instance_name']} to job {job_id}.")
            return node_doc["target_parallelism"]
        else:
            return 0

    with ThreadPoolExecutor() as executor:
        current_parallelism = sum(list(executor.map(assign_node, nodes_to_assign)))

    new_job_info = {"current_parallelism": Increment(current_parallelism)}
    add_background_task(job_ref.update, new_job_info)

    if current_parallelism == 0:
        add_background_task(reboot_nodes_with_job, DB, job_id)
        add_background_task(reconcile, DB, logger, add_background_task)
        return Response(status_code=500)  # , content="no ready nodes available.")

    job_ref.update({"benchmark.create_job_response": time()})
    return {"job_id": job_id}


@router.get("/v1/jobs/{job_id}")
def get_job_info(
    job_id: str = Path(...),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    job_ref = DB.collection("jobs").document(job_id)
    job = job_ref.get().to_dict()

    # udf_error ?  (error in user's code)
    if job.get("udf_errors"):
        # TODO: Send all errors (& indicies) if there are multiple ! (instead of just first error)
        input_index = job["udf_errors"][0]["input_index"]
        udf_error = job["udf_errors"][0]["udf_error"]
        logger.log(f"received an error in UDF with input at index {input_index} for job `{job_id}`")
        return {"udf_error": udf_error}

    # install_error ?  (error in user's environment)
    install_error = job.get("env", {}).get("install_error")
    if install_error:
        logger.log(f"received install error for job `{job_id}`")
        return {"install_error": install_error}

    # server error ?  (error in burla's code)
    cluster = Cluster.from_database(db=DB, logger=logger, add_background_task=add_background_task)
    if cluster.status(job_id=job_id) == "FAILED":
        logger.log(f"Received a server error (not a UDF error) for job `{job_id}`!")
        return Response(status_code=500)


@router.post("/v1/jobs/{job_id}/ended")
def report_job_ended(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    add_background_task(reconcile, DB, logger, add_background_task)
    reboot_nodes_with_job(DB, job_id)

    # Record/print metrics
    job_ref = DB.collection("jobs").document(job_id)
    benchmark = job_ref.get().to_dict()["benchmark"]
    benchmark.update(request_json.items())

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
    print(f"Total e2e runtime:{padding}\t{benchmark['job_ended_ts']-rpm_call_time}s\n")

    return {}
