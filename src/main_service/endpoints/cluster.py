import json
import asyncio
from time import time
from typing import Callable
from uuid import uuid4

import slack_sdk
from fastapi import APIRouter, Depends
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from starlette.responses import StreamingResponse
from concurrent.futures import ThreadPoolExecutor

from main_service import DB, IN_PROD, PROJECT_ID, get_logger, get_add_background_task_function
from main_service.cluster import reconcile
from main_service.node import Container, Node, JustifiedDeletion
from main_service.helpers import Logger, get_secret

router = APIRouter()


@router.post("/v1/cluster/restart")
def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()
    instance_client = InstancesClient()

    if IN_PROD:
        client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
        client.chat_postMessage(channel="user-activity", text="Someone started the prod cluster.")

    futures = []
    executor = ThreadPoolExecutor(max_workers=32)

    # delete all nodes
    instances_pager = instance_client.aggregated_list(project=PROJECT_ID)
    instances = [i for _, scope in instances_pager for i in getattr(scope, "instances", [])]
    burla_instances = [i for i in instances if "burla-cluster-node" in i.tags.items]
    for i in burla_instances:
        zone = i.zone.split("/")[-1]
        logger.log(f"Cluster is restarting: deleting node {i.name}")
        DB.collection("nodes").document(i.name).update({"status": "DELETED"})
        f = executor.submit(instance_client.delete, project=PROJECT_ID, zone=zone, instance=i.name)
        futures.append(f)

    # add nodes according to cluster_config doc
    def _add_node_logged(machine_type, containers, inactivity_time):
        instance_name = f"burla-node-{uuid4().hex}"
        logger.log(f"Cluster is restarting: adding node {instance_name}")
        try:
            Node.start(
                db=DB,
                logger=logger,
                add_background_task=add_background_task,
                machine_type=machine_type,
                instance_name=instance_name,
                containers=containers,
                inactivity_shutdown_time_sec=inactivity_time,
            )
        except JustifiedDeletion:
            # Thrown in Node constructor, means node was intentionally/correctly deleted mid-boot.
            pass

    config = DB.collection("cluster_config").document("cluster_config").get().to_dict()
    for node_spec in config["Nodes"]:
        for _ in range(node_spec["quantity"]):
            machine_type = node_spec["machine_type"]
            containers = [Container.from_dict(c) for c in node_spec["containers"]]
            inactivity_time = node_spec.get("inactivity_shutdown_time_sec")
            future = executor.submit(_add_node_logged, machine_type, containers, inactivity_time)
            futures.append(future)

    # wait until all operations done
    [future.result() for future in futures]
    executor.shutdown(wait=True)

    logger.log("Done restarting, reconciling ...")
    add_background_task(reconcile, DB, logger, add_background_task)

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")


@router.get("/v1/cluster")
async def cluster_info():
    node_name_to_status = {}

    async def node_stream():
        while True:
            status_filter = FieldFilter("status", "not-in", ["DELETED", "DELETING", "FAILED"])
            node_docs = list(DB.collection("nodes").where(filter=status_filter).stream())
            nodes = [doc.to_dict() for doc in node_docs]
            node_names = [node["instance_name"] for node in nodes]
            names_of_deleted_nodes = set(node_name_to_status.keys()) - set(node_names)

            # brodcast deleted nodes:
            for node_name in names_of_deleted_nodes:
                event_data = dict(nodeId=node_name, deleted=True)
                yield f"data: {json.dumps(event_data)}\n\n"
                del node_name_to_status[node_name]
                print(f"deleted node: {event_data}")

            # brodcast status updates:
            for node in nodes:
                instance_name = node["instance_name"]
                current_status = node["status"]
                previous_status = node_name_to_status.get(instance_name)

                if current_status != previous_status:
                    node_name_to_status[instance_name] = current_status
                    event_data = dict(nodeId=instance_name, status=current_status)
                    yield f"data: {json.dumps(event_data)}\n\n"
                    print(f"updated status: {event_data}")

            await asyncio.sleep(1)

    return StreamingResponse(node_stream(), media_type="text/event-stream")
