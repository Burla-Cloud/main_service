import json
import requests
from time import time
from uuid import uuid4
from typing import Optional, Callable
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, Path, Depends
from fastapi.responses import JSONResponse, Response
from google.cloud.firestore import FieldFilter, Increment


from main_service import (
    DB,
    get_user_email,
    get_logger,
    get_request_json,
    get_request_files,
    get_add_background_task_function,
)
from main_service.cluster import (
    parallelism_capacity,
    reboot_nodes_with_job,
    async_ensure_reconcile,
)
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
    validate_create_job_request(request_json)

    node_filter = FieldFilter("status", "==", "READY")
    ready_nodes = [n.to_dict() for n in DB.collection("nodes").where(filter=node_filter).stream()]
    if len(ready_nodes) == 0:
        msg = "Zero nodes with state `READY` are currently available."
        content = {"error_type": "NoReadyNodes", "message": msg}
        return JSONResponse(content=content, status_code=503)

    planned_future_job_parallelism = 0
    nodes_to_assign = []
    for node in ready_nodes:
        parallelism_deficit = request_json["max_parallelism"] - planned_future_job_parallelism
        max_node_parallelism = parallelism_capacity(
            node["machine_type"], request_json["func_cpu"], request_json["func_ram"]
        )

        if max_node_parallelism > 0 and parallelism_deficit > 0:
            node_target_parallelism = min(parallelism_deficit, max_node_parallelism)
            node["target_parallelism"] = node_target_parallelism
            node["starting_index"] = planned_future_job_parallelism  # idx to start work at
            planned_future_job_parallelism += node_target_parallelism
            nodes_to_assign.append(node)

    job_id = str(uuid4())
    job_ref = DB.collection("jobs").document(job_id)
    job_ref.set(
        {
            "n_inputs": int(request_json["n_inputs"]),
            "inputs_id": request_json["inputs_id"],
            "func_cpu": request_json["func_cpu"],
            "func_ram": request_json["func_ram"],
            "burla_client_version": request_json["burla_version"],
            "user_python_version": request_json["python_version"],
            "target_parallelism": request_json["max_parallelism"],
            "current_parallelism": 0,
            "planned_future_job_parallelism": planned_future_job_parallelism,
            "user": user_email,
            "started_at": time(),
            "udf_errors": [],
        }
    )

    if len(nodes_to_assign) == 0:
        content = {"error_type": "NoCompatibleNodes", "message": "No compatible nodes available."}
        return JSONResponse(content=content, status_code=503)

    if request_json["max_parallelism"] > planned_future_job_parallelism:
        # TODO: start more nodes here to fill the gap ?
        parallelism_deficit = request_json["max_parallelism"] - planned_future_job_parallelism
        msg = f"Cluster needs {parallelism_deficit} more cpus, "
        msg += f"continuing with a parallelism of {planned_future_job_parallelism}."
        logger.log(msg, severity="WARNING")

    # concurrently ask all ready nodes to start work:
    def assign_node(node: dict):
        """Errors in here are raised correctly!"""
        parallelism = node["target_parallelism"]
        starting_index = node["starting_index"]
        payload = {"parallelism": parallelism, "starting_index": starting_index}
        data = dict(request_json=json.dumps(payload))
        files = dict(function_pkl=request_files["function_pkl"])
        response = requests.post(f"{node['host']}/jobs/{job_id}", files=files, data=data)

        try:
            response.raise_for_status()
        except Exception as e:
            # Any errors returned here should also be raised inside the node service.
            # Errors here shouldn't kill the job because some workers are often able to start.
            # Nodes returning errors here should be restarted.
            msg = f"Node {node['instance_name']} refused job with error: {e}"
            logger.log(msg, severity="WARNING")
            return 0

        logger.log(f"Assigned node {node['instance_name']} to job {job_id}.")
        return node["target_parallelism"]

    with ThreadPoolExecutor(max_workers=32) as executor:
        current_parallelism = sum(list(executor.map(assign_node, nodes_to_assign)))

    if current_parallelism == 0:
        add_background_task(reboot_nodes_with_job, DB, job_id)
        async_ensure_reconcile(DB, logger, add_background_task)
        content = {"error_type": "JobRefused", "message": "Job refused by all available nodes."}
        return JSONResponse(content=content, status_code=503)
    else:
        new_job_info = {"current_parallelism": Increment(current_parallelism)}
        add_background_task(job_ref.update, new_job_info)
        return {"job_id": job_id}


@router.get("/v1/jobs/{job_id}")
def run_job_healthcheck(
    job_id: str = Path(...),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    # if not already happening, modify current cluster state -> correct/optimal state:
    async_ensure_reconcile(DB, logger, add_background_task)

    all_workers_done = False
    any_workers_failed = False
    workers_done_but_job_isnt = False

    # check status of every node / worker working on this job
    _filter = FieldFilter("current_job", "==", job_id)
    nodes_with_job = [n.to_dict() for n in DB.collection("nodes").where(filter=_filter).stream()]
    no_nodes_working_on_job = len(nodes_with_job) == 0
    for node in nodes_with_job:
        response = requests.get(f"{node['host']}/jobs/{job_id}")
        response.raise_for_status()
        any_workers_failed = response.json()["any_workers_failed"]
        all_workers_done = response.json()["all_workers_done"]

    # This should almost never be true because the client should stop doing healthchecks
    # before it is. Also because nodes restart themself when they are done, unassigning their job.
    # If it is true check if all outputs are in the DB, if not something is wrong!
    if all_workers_done:
        job_doc_ref = DB.collection("jobs").document(job_id)
        n_inputs = job_doc_ref.get().to_dict()["n_inputs"]
        n_outputs = job_doc_ref.collection("outputs").count().get()[0][0].value
        workers_done_but_job_isnt = n_inputs != n_outputs

    if no_nodes_working_on_job or any_workers_failed or workers_done_but_job_isnt:
        return Response(status_code=500)
